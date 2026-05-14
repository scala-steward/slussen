# slussen

A Scala 3 [direct-style](https://github.com/lampepfl/gears) Redis client. Slussen is a thin layer over [Lettuce](https://github.com/redis/lettuce) — Lettuce does the protocol, Netty I/O, pipelining, and pub/sub; [gears](https://github.com/lampepfl/gears) does the suspension and structured concurrency; slussen bridges the two.

No monads, no `F[_]`, no `IO`. Plain values, plain exceptions, `using Async`.

## Add to your build

```scala
libraryDependencies ++= Seq(
  "ch.epfl.lamp" %% "gears"        % "0.3.1",
  "io.lettuce"    % "lettuce-core" % "6.5.0.RELEASE"
)
```

Requires JDK 21+ and Scala 3.

## GET / SET

```scala
import gears.async.*
import gears.async.default.given
import io.lettuce.core.RedisURI
import io.lettuce.core.codec.StringCodec
import slussen.*

@main def example(): Unit = Async.blocking:
  Slussen.client(RedisURI.create("redis://localhost:6379")).use { client =>
    Slussen.connect(client, StringCodec.UTF8).use { conn =>
      val commands = conn.async()
      commands.set("hello", "world").await
      val value = commands.get("hello").await
      println(value) // "world"
    }
  }
```

`.await` suspends the current gears fiber; `.asFuture` is also available if you want to `race`/`zip` Redis calls with other futures.

## Pub/sub

```scala
import gears.async.*
import gears.async.default.given
import io.lettuce.core.RedisURI
import io.lettuce.core.codec.StringCodec
import slussen.*

@main def listener(): Unit = Async.blocking:
  Slussen.client(RedisURI.create("redis://localhost:6379")).use { client =>
    Slussen.subscribe(client, StringCodec.UTF8, "events").use { channel =>
      while true do
        val msg = channel.read()
        println(s"got ${msg.body} on ${msg.channel}")
    }
  }
```

The returned channel is an unbounded gears channel populated by Lettuce's Netty event-loop thread, so a slow consumer never blocks Netty — but a permanently slow consumer can grow memory without bound.

## Cancellation caveat

When the surrounding `Async` scope is cancelled, slussen calls `.cancel(true)` on the underlying Lettuce future. If the Redis command had already been written to the socket, **the server may still execute it** — there is no `UNSEND` in the protocol. Idempotent commands or `MULTI` are the standard remedies.

## What slussen does not do

Slussen is intentionally minimal. For anything beyond the bridge — full command surface, cluster, sentinel, master/replica, transactions, Lua scripting, connection pooling, pattern subscribe — call Lettuce directly. The Lettuce docs are at https://redis.github.io/lettuce/.
