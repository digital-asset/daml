// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.api.MetricHandle
import com.digitalasset.canton.config
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

// StreamTracker allows you to manage a workflow where you send a message,
// and wait for a corresponding message to be returned on a separate stream.
//
// The result stream must initially be instrumented to call `onStreamItem` on each item returned.
// Thereafter `track` may be called, passing the initiatiating action and a key, and returning a Future.
// The Future will return the first subsequent stream item which corresponds to the tracked key.
trait StreamTracker[K, I] extends AutoCloseable {
  def track(
      key: K,
      timeout: NonNegativeFiniteDuration,
  )(
      start: TraceContext => FutureUnlessShutdown[Any]
  )(implicit
      ec: ExecutionContext,
      errorLogger: ErrorLoggingContext,
      traceContext: TraceContext,
      tracer: Tracer,
      errors: StreamTracker.Errors[K],
  ): Future[I]

  def onStreamItem(item: I): Unit
}

object StreamTracker {
  def withTimer[Key, Item](
      timer: java.util.Timer,
      itemKey: Item => Option[Key],
      inFlightCounter: InFlight,
      loggerFactory: NamedLoggerFactory,
  ): StreamTracker[Key, Item] =
    new StreamTrackerImpl(
      new CancellableTimeoutSupportImpl(timer, loggerFactory),
      itemKey,
      inFlightCounter,
      loggerFactory,
    )

  def owner[Key, Item](
      trackerThreadName: String,
      itemKey: Item => Option[Key],
      inFlightCounter: InFlight,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[StreamTracker[Key, Item]] =
    for {
      timeoutSupport <- CancellableTimeoutSupport
        .owner(s"$trackerThreadName-timeout-timer", loggerFactory)
      streamTracker <- ResourceOwner.forCloseable(() =>
        new StreamTrackerImpl(
          timeoutSupport,
          itemKey,
          inFlightCounter,
          loggerFactory,
        )
      )
    } yield streamTracker

  trait Errors[Key] {
    def timedOut(key: Key)(implicit errorLogger: ErrorLoggingContext): StatusRuntimeException
    def duplicated(key: Key)(implicit
        errorLogger: ErrorLoggingContext
    ): StatusRuntimeException
  }
}

private[tracking] class StreamTrackerImpl[Key, Item](
    cancellableTimeoutSupport: CancellableTimeoutSupport,
    itemKey: Item => Option[Key],
    inFlightCounter: InFlight,
    val loggerFactory: NamedLoggerFactory,
) extends StreamTracker[Key, Item]
    with NamedLogging
    with Spanning {

  private[tracking] val pending =
    TrieMap.empty[Key, (ErrorLoggingContext, Promise[Item])]

  override def track(
      key: Key,
      timeout: NonNegativeFiniteDuration,
  )(
      start: TraceContext => FutureUnlessShutdown[Any]
  )(implicit
      ec: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
      tracer: Tracer,
      errors: StreamTracker.Errors[Key],
  ): Future[Item] =
    inFlightCounter.check(pending.size) {
      val promise = Promise[Item]()
      pending.putIfAbsent(key, (errorLoggingContext, promise)) match {
        case Some(_) => promise.failure(errors.duplicated(key)(errorLoggingContext))
        case None => trackWithCancelTimeout(key, timeout, promise, start)
      }
      promise.future
    }

  private def trackWithCancelTimeout(
      key: Key,
      timeout: config.NonNegativeFiniteDuration,
      promise: Promise[Item],
      start: TraceContext => FutureUnlessShutdown[Any],
  )(implicit
      ec: ExecutionContext,
      errorLogger: ErrorLoggingContext,
      traceContext: TraceContext,
      tracer: Tracer,
      errors: StreamTracker.Errors[Key],
  ): Unit =
    Try(
      // Start the timeout timer before start to ensure that the timer scheduling
      // happens before its cancellation (on start failure OR onStreamItem)
      cancellableTimeoutSupport.scheduleOnce(
        duration = timeout,
        promise = promise,
        onTimeout = Failure(errors.timedOut(key)(errorLogger)),
      )
    ) match {
      case Failure(err) =>
        logger.error(
          "An internal error occurred while trying to register the cancellation timeout. Aborting..",
          err,
        )
        pending.remove(key).discard
        promise.tryFailure(err).discard
      case Success(cancelTimeout) =>
        withSpan("StreamTracker.track") { childContext => _ =>
          start(childContext)
            .onComplete {
              case Success(_) => // succeeded, nothing to do
              case Failure(throwable) =>
                // Start failed, finishing entry with the very same error
                promise.tryComplete(Failure(throwable)).discard[Boolean]
            }
        }
        promise.future.onComplete { _ =>
          // register timeout cancellation and removal from map
          withSpan("StreamTracker.complete") { _ => _ =>
            cancelTimeout.close()
            pending.remove(key)
          }
        }
    }

  override def onStreamItem(item: Item): Unit =
    itemKey(item).flatMap(pending.get(_)).foreach { case (_traceCtx, promise) =>
      promise.tryComplete(Success(item)).discard
    }

  override def close(): Unit =
    pending.values.foreach { case (traceCtx, promise) =>
      promise.tryFailure(CommonErrors.ServerIsShuttingDown.Reject()(traceCtx).asGrpcError).discard
    }
}

trait InFlight {
  def check[T](currCount: Int)(
      f: => Future[T]
  )(implicit ec: ExecutionContext, errorLogger: ErrorLoggingContext): Future[T]
}

object InFlight {
  final case class Limited(maxCount: Int, metric: MetricHandle.Counter) extends InFlight {
    import com.digitalasset.canton.ledger.error.LedgerApiErrors

    def check[T](currCount: Int)(
        f: => Future[T]
    )(implicit
        ec: ExecutionContext,
        errorLogger: ErrorLoggingContext,
    ): Future[T] =
      if (currCount < maxCount) {
        metric.inc()
        f.thereafter(_ => metric.dec())
      } else {
        Future.failed(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("Maximum number of in-flight requests reached")
            .asGrpcError
        )
      }
  }

  object Unlimited extends InFlight {
    def check[T](currCount: Int)(
        f: => Future[T]
    )(implicit
        ec: ExecutionContext,
        errorLogger: ErrorLoggingContext,
    ): Future[T] = f
  }
}
