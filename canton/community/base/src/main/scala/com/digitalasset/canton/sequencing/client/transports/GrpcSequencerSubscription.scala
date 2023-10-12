// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.NamedLogging.loggerWithoutTracing
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.{SequencerSubscription, SubscriptionCloseReason}
import com.digitalasset.canton.sequencing.protocol.SubscriptionResponse
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.tracing.TraceContext.withTraceContext
import com.digitalasset.canton.tracing.{NoTracing, SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.FutureUtil
import com.google.common.annotations.VisibleForTesting
import io.grpc.Context.CancellableContext
import io.grpc.Status.Code.CANCELLED
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.*
import scala.util.Try

/** Supply the grpc error that caused the subscription to fail */
final case class GrpcSubscriptionError(grpcError: GrpcError)
    extends SubscriptionCloseReason.SubscriptionError

/** Supply the grpc error and specially tag permission denied issues */
final case class GrpcPermissionDeniedError(grpcError: GrpcError)
    extends SubscriptionCloseReason.PermissionDeniedError

/** The GPRC subscription observer was called with an unexpected exception */
final case class GrpcSubscriptionUnexpectedException(exception: Throwable)
    extends SubscriptionCloseReason.SubscriptionError

trait HasProtoTraceContext[R] {
  def traceContext(value: R): Option[com.digitalasset.canton.v0.TraceContext]
}
object HasProtoTraceContext {
  implicit val subscriptionResponseTraceContext =
    new HasProtoTraceContext[v0.SubscriptionResponse] {
      override def traceContext(value: v0.SubscriptionResponse) = value.traceContext
    }

  implicit val versionedSubscriptionResponseTraceContext =
    new HasProtoTraceContext[v0.VersionedSubscriptionResponse] {
      override def traceContext(value: v0.VersionedSubscriptionResponse) = value.traceContext
    }
}

@VisibleForTesting
class GrpcSequencerSubscription[E, R: HasProtoTraceContext] private[transports] (
    context: CancellableContext,
    callHandler: Traced[R] => Future[Either[E, Unit]],
    metrics: SequencerClientMetrics,
    override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerSubscription[E]
    with NoTracing // tracing details are serialized within the items handled inside onNext
    {

  /** Stores ongoing work performed by `onNext` or `complete`.
    * The contained future is completed whenever these methods are not busy.
    */
  private val currentProcessing = new AtomicReference[Future[Unit]](Future.unit)
  private val currentAwaitOnNext = new AtomicReference[Promise[UnlessShutdown[Either[E, Unit]]]](
    Promise.successful(Outcome(Right(())))
  )

  runOnShutdown_(new RunOnShutdown {
    override def name: String = "cancel-current-await-in-onNext"
    override def done: Boolean = currentAwaitOnNext.get.isCompleted
    override def run(): Unit = currentAwaitOnNext.get.trySuccess(AbortedDueToShutdown).discard
  })

  private val cancelledByClient = new AtomicBoolean(false)

  private def cancel(): Unit =
    if (!cancelledByClient.getAndSet(true)) context.close()

  private def appendToCurrentProcessing(next: Try[Unit] => Future[Unit]): Unit = {
    val newPromise = Promise[Unit]()
    val oldFuture = currentProcessing.getAndSet(newPromise.future)
    newPromise.completeWith(oldFuture.transformWith { outcome =>
      FutureUtil.logOnFailure(
        Future.fromTry(Try(next(outcome))).flatten,
        "An unexpected exception has occurred in currentProcessing.",
      )
    })
  }

  override private[canton] def complete(
      result: SubscriptionCloseReason[E]
  )(implicit traceContext: TraceContext): Unit = {
    // Make sure that result is emitted, once the current processing has completed.
    appendToCurrentProcessing { outcome =>
      val completion = outcome.map(_ => result)
      if (closeReasonPromise.tryComplete(completion)) {
        logger.debug(s"Completed subscription with $completion")
      } else {
        logger.debug(s"Already completed. Discarding $result")
      }
      Future.unit
    }

    // Make sure that no further events will be processed
    cancel()
    close()
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    // Signal termination by client
    val completionF = Future { complete(SubscriptionCloseReason.Closed) }
    val onTimeout = (ex: TimeoutException) => {
      logger.warn(s"Clean close of the ${this.getClass} timed out", ex)
      closeReasonPromise.tryFailure(ex).discard[Boolean]
    }

    /*
      Potential reason of failure to close within the timeout: current processing
      is blocked, probably because the `callHandler` is not making progress.
      For example, this might happen due to a db outage.
     */
    Seq(
      SyncCloseable("grpc-context", cancel()), // Tell GRPC to stop receiving messages
      AsyncCloseable(
        "grpc-sequencer-subscription",
        completionF,
        timeouts.shutdownShort.duration,
        onTimeout = onTimeout,
      ),
    )
  }

  @VisibleForTesting // so unit tests can call onNext, onError and onComplete
  private[transports] val observer = new StreamObserver[R] {
    override def onNext(value: R): Unit = {
      // we take the unusual step of immediately trying to deserialize the trace-context
      // so it is available here for logging
      implicit val traceContext: TraceContext =
        SerializableTraceContext
          .fromProtoSafeV0Opt(loggerWithoutTracing(logger))(
            implicitly[HasProtoTraceContext[R]].traceContext(value)
          )
          .unwrap

      logger.debug("Received a message from the sequencer.")

      val current = Promise[Unit]()
      val closeReasonOO = performUnlessClosing(functionFullName) {
        try {
          appendToCurrentProcessing(_ => current.future)

          // as we're responsible for calling the handler we block onNext from processing further items
          // calls to onNext are guaranteed to happen in order

          val handlerResult = Try {
            val cancelableAwait = Promise[UnlessShutdown[Either[E, Unit]]]()
            currentAwaitOnNext.set(cancelableAwait)
            cancelableAwait.completeWith(callHandler(Traced(value)).map(Outcome(_)))
            timeouts.unbounded
              .await(s"${this.getClass}: Blocking processing of further items")(
                cancelableAwait.future
              )
          }

          handlerResult.fold[Option[SubscriptionCloseReason[E]]](
            ex => Some(SubscriptionCloseReason.HandlerException(ex)),
            {
              case Outcome(Left(err)) =>
                Some(SubscriptionCloseReason.HandlerError(err))
              case Outcome(Right(_)) =>
                // we'll continue
                None
              case AbortedDueToShutdown =>
                Some(SubscriptionCloseReason.Shutdown)
            },
          )
        } finally current.success(())
      }

      // if a close reason was returned, close the subscription
      closeReasonOO match {
        case UnlessShutdown.Outcome(maybeCloseReason) =>
          maybeCloseReason.foreach(complete)
          logger.debug("Finished processing of the sequencer message.")
        case UnlessShutdown.AbortedDueToShutdown =>
          logger.debug(s"The message is not processed, as the node is closing.")
      }
    }

    override def onError(t: Throwable): Unit = {
      t match {
        case s: StatusRuntimeException if s.getStatus.getCode == CANCELLED =>
          if (cancelledByClient.get()) {
            logger.info(
              "GRPC subscription successfully closed due to client shutdown.",
              s.getStatus.getCause,
            )
            complete(SubscriptionCloseReason.Closed)
          } else {
            // As the client has not cancelled the subscription, the problem must be on the server side.
            val grpcError =
              GrpcServiceUnavailable(
                "subscription",
                "sequencer",
                s.getStatus,
                Option(s.getTrailers),
                None,
              )
            complete(GrpcSubscriptionError(grpcError))
          }
        case s: StatusRuntimeException =>
          val grpcError = GrpcError("subscription", "sequencer", s)
          complete(
            if (s.getStatus.getCode == Status.Code.PERMISSION_DENIED)
              GrpcPermissionDeniedError(grpcError)
            else GrpcSubscriptionError(grpcError)
          )
        case exception: Throwable =>
          logger.error("The sequencer subscription failed unexpectedly.", t)
          complete(GrpcSubscriptionUnexpectedException(exception))
      }
    }

    override def onCompleted(): Unit = {
      // Info level, as this occurs from time to time due to the invalidation of the authentication token.
      logger.info("The sequencer subscription has been terminated by the server.")
      complete(
        GrpcSubscriptionError(
          GrpcError(
            "subscription",
            "sequencer",
            Status.UNAVAILABLE
              .withDescription("Connection terminated by the server.")
              .asRuntimeException(),
          )
        )
      )
    }

  }
}

object GrpcSequencerSubscription {
  def fromSubscriptionResponse[E](
      context: CancellableContext,
      handler: SerializedEventHandler[E],
      metrics: SequencerClientMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): GrpcSequencerSubscription[E, v0.SubscriptionResponse] =
    new GrpcSequencerSubscription(
      context,
      deserializingSubscriptionHandler(
        handler,
        (value, traceContext) => SubscriptionResponse.fromProtoV0(value)(traceContext),
      ),
      metrics,
      timeouts,
      loggerFactory,
    )

  def fromVersionedSubscriptionResponse[E](
      context: CancellableContext,
      handler: SerializedEventHandler[E],
      metrics: SequencerClientMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): GrpcSequencerSubscription[E, v0.VersionedSubscriptionResponse] =
    new GrpcSequencerSubscription(
      context,
      deserializingSubscriptionHandler(
        handler,
        (value, traceContext) => SubscriptionResponse.fromVersionedProtoV0(value)(traceContext),
      ),
      metrics,
      timeouts,
      loggerFactory,
    )

  private def deserializingSubscriptionHandler[E, R](
      handler: SerializedEventHandler[E],
      fromProto: (R, TraceContext) => ParsingResult[SubscriptionResponse],
  ): Traced[R] => Future[Either[E, Unit]] = {
    withTraceContext { implicit traceContext => responseP =>
      fromProto(responseP, traceContext)
        .fold(
          err =>
            Future.failed(
              new RuntimeException(
                s"Unable to parse response from sequencer. Discarding message. Reason: $err"
              )
            ),
          response => {
            val signedEvent = response.signedSequencedEvent
            val ordinaryEvent =
              OrdinarySequencedEvent(signedEvent, response.trafficState)(response.traceContext)
            handler(ordinaryEvent)
          },
        )
    }
  }
}
