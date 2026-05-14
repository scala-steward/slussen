# Slussen — Design Spec

**Date:** 2026-05-14
**Status:** Approved for v1 plan

## Summary

Slussen is a Scala 3 + [gears](https://github.com/lampepfl/gears) **direct-style** wrapper on top of [Lettuce](https://github.com/redis/lettuce), the Java Redis client. It exposes Lettuce's full command surface to Scala 3 code using gears' `Async` capability — no monads, no `F[_]`, no `IO`.

v1 is **bridge-only**: we ship the conversions and lifecycle wrappers needed to use Lettuce from direct-style Scala, and nothing else. There is no hand-written command-by-command facade. The user calls Lettuce's `RedisAsyncCommands` directly and awaits the resulting `CompletionStage`.

Inspiration from [redis4cats](https://github.com/profunktor/redis4cats) is for API ergonomics (Resource shape, method names), **not** for its cats-effect runtime.

## Non-goals (v1)

- No RESP parser, no TCP transport, no NIO bridge — Lettuce provides these.
- No hand-written command facade (no `Slussen.get(k)`, etc.).
- **No tests.**
- No retries, no circuit breaker, no timeout helper.
- No cluster/sentinel helpers (Lettuce provides; users can call directly).
- No connection pool wrapper.
- No pattern subscribe (`PSUBSCRIBE`).
- No cross-build to Scala Native / Scala.js (Lettuce is JVM-only via Netty).
- No published Maven artifact.

## Stack

- **Scala 3** (latest 3.x at implementation time; we target `-new-syntax`).
- **sbt** as build tool.
- **JDK 21+** (required by gears).
- **gears** for direct-style concurrency (`Async`, `Future`, `Resource`, `UnboundedChannel`).
- **Lettuce 6.x** for the underlying Redis transport.
- **scalafmt** with two-space indentation.
- Package: bare `slussen` (no vendor namespace in v1).

## Architecture

Three logical components in one sbt module sitting on top of Lettuce and gears:

```
            ┌────────────── slussen ──────────────┐
            │                                     │
   user ──► │  AwaitOps     ResourceOps   PubSubOps │
            │   (1)            (2)           (3)    │
            └──────┬────────────┬──────────────┬───┘
                   │            │              │
              CompletionStage   │       RedisPubSubListener
                   │            │              │
                   ▼            ▼              ▼
            ┌─────── Lettuce (Java) ──────────────┐
            │  RedisClient · StatefulRedisConn …  │
            └──────────────────────────────────────┘
                            │
                          Netty (TCP, RESP)
```

1. **AwaitOps** — `CompletionStage[T]` ⇒ direct-style. Extension methods only.
2. **ResourceOps** — `RedisClient` and `StatefulRedisConnection` as gears `Resource`.
3. **PubSubOps** — `RedisPubSubListener` ⇒ gears `UnboundedChannel`, as a `Resource`.

Three separate files because the three responsibilities touch different Lettuce surfaces and have different cancellation semantics. `AwaitOps` is reusable for any `CompletionStage`, not just Redis, which keeps the surface honest.

## Components

### File layout

```
slussen/
├── build.sbt
├── project/build.properties
├── .scalafmt.conf
├── README.md
└── src/main/scala/slussen/
    ├── package.scala              # package object: re-exports
    ├── AwaitOps.scala             # extension methods on CompletionStage[T]
    ├── ResourceOps.scala          # client + connect Resources
    ├── PubSubOps.scala            # subscribe Resource
    └── internal/
        └── ListenerAdapter.scala  # RedisPubSubListener → UnboundedChannel
```

Total expected size: ~150 lines of Scala.

### `AwaitOps.scala`

Pure conversion, no Lettuce imports. Sketch:

```scala
package slussen

import gears.async.*
import java.util.concurrent.{CompletionException, CompletionStage}

object AwaitOps:
  extension [T](cs: CompletionStage[T])
    def await(using Async): T =
      cs.asFuture.await

    def asFuture(using Async, Async.Spawn): Future[T] =
      Future.withResolver[T] { r =>
        cs.whenComplete { (v, err) =>
          if err == null then r.resolve(v)
          else r.reject(unwrap(err))
        }
        r.onCancel { () =>
          cs.toCompletableFuture.cancel(true)  // best-effort
          ()
        }
      }

  private def unwrap(t: Throwable): Throwable = t match
    case ce: CompletionException if ce.getCause != null => ce.getCause
    case other => other
```

### `ResourceOps.scala`

```scala
package slussen

import gears.async.Resource
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec

object ResourceOps:
  def client(uri: RedisURI): Resource[RedisClient] =
    Resource.make(RedisClient.create(uri))(_.shutdown())

  def connect[K, V](
      c: RedisClient,
      codec: RedisCodec[K, V]
  ): Resource[StatefulRedisConnection[K, V]] =
    Resource.make(c.connect(codec))(_.close())
```

(Note: the exact gears `Resource.make` signature may use different param names; the implementer should confirm against the gears version pulled in. The shape is the same.)

### `PubSubOps.scala` + `internal/ListenerAdapter.scala`

```scala
package slussen

import gears.async.*
import io.lettuce.core.RedisClient
import io.lettuce.core.codec.RedisCodec
import slussen.internal.ListenerAdapter

final case class Message[K, V](channel: K, body: V)

object PubSubOps:
  def subscribe[K, V](
      c: RedisClient,
      codec: RedisCodec[K, V],
      channels: K*
  ): Resource[ReadableChannel[Message[K, V]]] =
    Resource.make {
      val conn = c.connectPubSub(codec)
      val ch   = UnboundedChannel[Message[K, V]]()
      conn.addListener(ListenerAdapter(ch))
      conn.sync().subscribe(channels.toArray*)
      (conn, ch)
    } { case (conn, _) =>
      try conn.sync().unsubscribe()
      finally conn.close()
    }.map(_._2)
```

```scala
package slussen.internal

import io.lettuce.core.pubsub.RedisPubSubListener
import gears.async.UnboundedChannel
import slussen.Message

private[slussen] final class ListenerAdapter[K, V](ch: UnboundedChannel[Message[K, V]])
    extends RedisPubSubListener[K, V]:

  override def message(channel: K, body: V): Unit =
    ch.sendImmediately(Message(channel, body))

  // Pattern subscribe not in v1.
  override def message(pattern: K, channel: K, body: V): Unit = ()
  override def subscribed(channel: K, count: Long): Unit       = ()
  override def psubscribed(pattern: K, count: Long): Unit      = ()
  override def unsubscribed(channel: K, count: Long): Unit     = ()
  override def punsubscribed(pattern: K, count: Long): Unit    = ()
```

## Data flow

### Command flow (request/response)

```
user code            slussen              Lettuce              Netty           Redis
   │                    │                    │                    │              │
   │ conn.async()       │                    │                    │              │
   │ .get("k").await    │                    │                    │              │
   ├───────────────────►│                    │                    │              │
   │              ┌─────┴─────┐              │                    │              │
   │              │ AwaitOps  │──get("k")───►│ RedisFuture        │              │
   │              │ (suspend) │              ├─────write────────► │ ─GET k──────►│
   │              │ via gears │              │                    │              │
   │              │  Future   │              │ ◄────callback───── │ ◄────────────│
   │              │           │◄─resolve(v)──│                    │              │
   │              └─────┬─────┘              │                    │              │
   │◄─── "value" ───────│                    │                    │              │
```

The calling fiber suspends inside `Future.withResolver`; `whenComplete` resolves it when Lettuce's `RedisFuture` completes. No callback chain in user code.

### Resource flow (lifecycle)

```scala
Async.blocking:
  Slussen.client(uri).use { client =>
    Slussen.connect(client, StringCodec.UTF8).use { conn =>
      conn.async().get("foo").await        // work happens here
    }                                       // conn.close() — fast
  }                                         // client.shutdown() — Netty quiet period
```

`Resource.use` from gears guarantees release on cancellation/exception. Creating a `RedisClient` is **expensive** (owns the Netty event-loop group); reuse one per app.

### Pub/sub flow

```
Async.blocking:
  Slussen.subscribe(client, codec, "events").use { ch =>
    while true do
      val msg = ch.read()                   // suspends until a message arrives
      handle(msg)
  }                                         // unsubscribe + close pubsub conn

   Netty event-loop thread ──► ListenerAdapter.message(channel, body)
                              └─► ch.sendImmediately(Message(channel, body))
                              (returns instantly, unbounded buffer, Netty never blocks)
```

The Netty thread *publishes* into the channel; user fibers *consume* from it. They never share a thread.

### Ordering guarantees

- Within one connection: Redis preserves command order, Lettuce preserves response order, slussen preserves fiber-completion order — a single fiber issuing sequential `.await` calls sees a serial transcript.
- Across fibers sharing one connection: Lettuce pipelines commands (interleaves on the wire). Both fibers see correct individual results, but there is **no cross-fiber atomicity**. Use `MULTI`/`EXEC` or scripts for that (out of v1 scope, but available via Lettuce directly).

## Error handling

Raw Lettuce exceptions are propagated as-is through `.await`. No wrapping, no sealed error hierarchy:

```scala
try conn.async().get("k").await
catch
  case e: RedisCommandExecutionException => …
  case e: RedisCommandTimeoutException   => …
  case e: RedisConnectionException       => …
```

Rationale: direct style + plain JVM exceptions is the point. Wrapping costs nothing on the happy path but loses information on the sad path.

**One translation:** `CompletionException` is unwrapped to its `getCause`. This is a `CompletableFuture` quirk; bridging is the right place to hide it.

## Cancellation

When the surrounding gears `Async` scope is cancelled, `Future.withResolver`'s `onCancel` callback fires and we call `cs.toCompletableFuture.cancel(true)`.

| Command state                | What slussen does | Server side             |
|------------------------------|--------------------|-------------------------|
| Still in Lettuce's queue     | Removed; never sent | Never executed          |
| Written to Netty, no response yet | Response slot dropped | **May still execute** |
| Response already arrived     | No-op              | Already executed        |

The middle row is the unavoidable Redis reality: once bytes are on the wire, there is no `UNSEND`. Documented on `.await`/`.asFuture`.

**Pub/sub cancellation:**
- Cancelling a fiber blocked on `read()` just unblocks it (gears handles this).
- Closing the pub/sub `Resource` calls `unsubscribe()` then `conn.close()`. Messages still in the unbounded buffer at close time are dropped silently.

## Build

`build.sbt`:

```scala
ThisBuild / scalaVersion := "3.5.2"   // latest 3.x at implementation time

lazy val slussen = (project in file("."))
  .settings(
    name := "slussen",
    libraryDependencies ++= Seq(
      "ch.epfl.lamp"  %% "gears"        % "<latest>",
      "io.lettuce"    %  "lettuce-core" % "6.5.x"
    ),
    scalacOptions ++= Seq("-new-syntax", "-Wunused:all")
  )
```

`.scalafmt.conf`:

```hocon
version = "<latest>"
runner.dialect = scala3
maxColumn = 100
indent.main = 2
```

No multi-module, no cross-build, no publish settings.

## README

Single page. Must include:
- One-paragraph "what is this": Lettuce + gears, direct style.
- Snippet showing `Async.blocking { client.use { … } }` with `GET`/`SET`.
- Snippet showing pub/sub.
- The cancellation caveat (in-flight commands may execute server-side).
- Pointer to Lettuce docs for the full command surface.

## Open questions (deferred, not blocking v1)

- Should `Slussen.client` and `Slussen.connect` live in a top-level `Slussen` object (re-export from `ResourceOps`/`PubSubOps`), or be called via the per-component objects? Decide during implementation; trivial to change.
- gears `Resource` API exact shape — confirm against pulled-in version. Sketches above assume `Resource.make(acquire)(release)` and `Resource[T].map`. If gears uses a different name (e.g., `Resource.fromAutoCloseable`), adapt directly.
- Latest gears version — pin at implementation time.

## Parking lot (post-v1)

- Tests (unit, integration).
- Connection pool helper (`ConnectionPoolSupport` wrapper).
- Pattern subscribe (`PSUBSCRIBE`).
- Cluster / sentinel / master-replica Resource builders.
- Timeout helper or alignment with gears' existing timeout combinators.
- Maven Central publish, cross-version setup.
- Sample app or examples module.
