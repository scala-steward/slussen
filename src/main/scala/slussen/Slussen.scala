package slussen

import gears.async.{ReadableChannel, Resource, UnboundedChannel}
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import slussen.internal.ListenerAdapter
import scala.reflect.ClassTag

final case class Message[K, V](channel: K, body: V)

object Slussen:

  def client(uri: RedisURI): Resource[RedisClient] =
    Resource(
      RedisClient.create(uri),
      c => c.shutdown()
    )

  def connect[K, V](
      c: RedisClient,
      codec: RedisCodec[K, V]
  ): Resource[StatefulRedisConnection[K, V]] =
    Resource(
      c.connect(codec),
      conn => conn.close()
    )

  def subscribe[K: ClassTag, V](
      c: RedisClient,
      codec: RedisCodec[K, V],
      channels: K*
  ): Resource[ReadableChannel[Message[K, V]]] =
    Resource(
      {
        val conn = c.connectPubSub(codec)
        val ch = UnboundedChannel[Message[K, V]]()
        conn.addListener(ListenerAdapter(ch))
        conn.sync().subscribe(channels.toArray*)
        (conn, ch)
      },
      (pair: (
          io.lettuce.core.pubsub.StatefulRedisPubSubConnection[K, V],
          UnboundedChannel[Message[K, V]]
      )) =>
        try pair._1.sync().unsubscribe()
        finally pair._1.close()
    ).map {
      (pair: (
          io.lettuce.core.pubsub.StatefulRedisPubSubConnection[K, V],
          UnboundedChannel[Message[K, V]]
      )) =>
        pair._2
    }

end Slussen
