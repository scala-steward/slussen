package slussen.internal

import gears.async.UnboundedChannel
import io.lettuce.core.pubsub.RedisPubSubListener
import slussen.Message

private[slussen] final class ListenerAdapter[K, V](
    ch: UnboundedChannel[Message[K, V]]
) extends RedisPubSubListener[K, V]:

  override def message(channel: K, body: V): Unit =
    ch.sendImmediately(Message(channel, body))

  override def message(pattern: K, channel: K, body: V): Unit = ()
  override def subscribed(channel: K, count: Long): Unit = ()
  override def psubscribed(pattern: K, count: Long): Unit = ()
  override def unsubscribed(channel: K, count: Long): Unit = ()
  override def punsubscribed(pattern: K, count: Long): Unit = ()
