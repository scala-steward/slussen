package slussen

import gears.async.Resource
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec

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

end Slussen
