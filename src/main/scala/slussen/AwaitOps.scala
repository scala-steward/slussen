package slussen

import gears.async.{Async, Future}
import java.util.concurrent.{CompletionException, CompletionStage}

extension [T](cs: CompletionStage[T])
  def await(using Async): T =
    cs.asFuture.await

  def asFuture: Future[T] =
    Future.withResolver[T]: r =>
      cs.whenComplete: (v, err) =>
        if err == null then r.resolve(v)
        else r.reject(unwrapCompletion(err))
      r.onCancel: () =>
        val _ = cs.toCompletableFuture.cancel(true)

private def unwrapCompletion(t: Throwable): Throwable = t match
  case ce: CompletionException if ce.getCause != null => ce.getCause
  case other => other
