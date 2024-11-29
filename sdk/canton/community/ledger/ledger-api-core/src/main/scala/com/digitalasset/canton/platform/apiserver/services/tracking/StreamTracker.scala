// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.api.MetricHandle.Counter
import com.digitalasset.canton.config
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
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
      start: TraceContext => FutureUnlessShutdown[Any],
  )(implicit
      ec: ExecutionContext,
      errorLogger: ContextualizedErrorLogger,
      traceContext: TraceContext,
      tracer: Tracer,
  ): Future[I]

  def onStreamItem(item: I): Unit
}

object StreamTracker {
  def owner[Key <: Errors.KeyDescriptions, Item](
      trackerThreadName: String,
      itemKey: Item => Key,
      maxInFlight: Int,
      inFlightCount: Counter,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[StreamTracker[Key, Item]] =
    for {
      timeoutSupport <- CancellableTimeoutSupport
        .owner(s"$trackerThreadName-timeout-timer", loggerFactory)
      streamTracker <- ResourceOwner.forCloseable(() =>
        new StreamTrackerImpl(
          timeoutSupport,
          itemKey,
          maxInFlight,
          inFlightCount,
          loggerFactory,
        )
      )
    } yield streamTracker
}

private[tracking] class StreamTrackerImpl[Key <: Errors.KeyDescriptions, Item](
    cancellableTimeoutSupport: CancellableTimeoutSupport,
    itemKey: Item => Key,
    maxInFlight: Int,
    inFlightCount: Counter,
    val loggerFactory: NamedLoggerFactory,
) extends StreamTracker[Key, Item]
    with NamedLogging
    with Spanning {

  private[tracking] val pending =
    TrieMap.empty[Key, (ContextualizedErrorLogger, Promise[Item])]

  def track(
      key: Key,
      timeout: NonNegativeFiniteDuration,
      start: TraceContext => FutureUnlessShutdown[Any],
  )(implicit
      ec: ExecutionContext,
      errorLogger: ContextualizedErrorLogger,
      traceContext: TraceContext,
      tracer: Tracer,
  ): Future[Item] =
    ensuringMaximumInFlight {
      val promise = Promise[Item]()
      pending.putIfAbsent(key, (errorLogger, promise)) match {
        case Some(_) => promise.failure(Errors.duplicated(key))
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
      errorLogger: ContextualizedErrorLogger,
      traceContext: TraceContext,
      tracer: Tracer,
  ): Unit =
    Try(
      // Start the timeout timer before start to ensure that the timer scheduling
      // happens before its cancellation (on start failure OR onStreamItem)
      cancellableTimeoutSupport.scheduleOnce(
        duration = timeout,
        promise = promise,
        onTimeout = Failure(Errors.timedOut(key)(errorLogger)),
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

  def onStreamItem(item: Item): Unit =
    pending.get(itemKey(item)).foreach { case (_traceCtx, promise) =>
      promise.tryComplete(scala.util.Success(item)).discard
    }

  override def close(): Unit =
    pending.values.foreach { case (traceCtx, promise) =>
      promise.tryFailure(Errors.closed(traceCtx)).discard
    }

  private def ensuringMaximumInFlight[T](
      f: => Future[T]
  )(implicit
      ec: ExecutionContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[T] =
    if (pending.sizeIs < maxInFlight) {
      inFlightCount.inc()
      f.thereafter(_ => inFlightCount.dec())
    } else {
      Future.failed(Errors.full(errorLogger))
    }
}

object Errors {
  import com.digitalasset.canton.ledger.error.CommonErrors
  import com.digitalasset.canton.ledger.error.LedgerApiErrors
  import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors

  trait KeyDescriptions {
    def streamItemDescription: String
    def requestDescription: String
    def requestId: String
  }

  def timedOut(key: KeyDescriptions)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Throwable =
    CommonErrors.RequestTimeOut
      .Reject(
        s"Timed out while awaiting for ${key.streamItemDescription} corresponding to ${key.requestDescription}.",
        definiteAnswer = false,
      )
      .asGrpcError

  def duplicated(
      key: KeyDescriptions
  )(implicit errorLogger: ContextualizedErrorLogger): Throwable =
    // TODO(i22596): Stop hard-coding this to DuplicateCommand
    ConsistencyErrors.DuplicateCommand
      .Reject(existingCommandSubmissionId = Some(key.requestId))
      .asGrpcError

  def closed(implicit errorLogger: ContextualizedErrorLogger): Throwable =
    CommonErrors.ServerIsShuttingDown.Reject().asGrpcError

  def full(implicit errorLogger: ContextualizedErrorLogger): Throwable =
    LedgerApiErrors.ParticipantBackpressure
      .Rejection("Maximum number of in-flight requests reached")
      .asGrpcError
}
