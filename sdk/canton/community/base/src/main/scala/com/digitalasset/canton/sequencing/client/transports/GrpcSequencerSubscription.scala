// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencing.SequencedEventHandler
import com.digitalasset.canton.sequencing.client.{SequencerSubscription, SubscriptionCloseReason}
import com.digitalasset.canton.sequencing.protocol.SubscriptionResponse
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.tracing.TraceContext.withTraceContext
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.version.ProtocolVersion
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
  def traceContext(value: R): Option[com.digitalasset.canton.v30.TraceContext]
}
object HasProtoTraceContext {
  implicit val versionedSubscriptionResponseTraceContext
      : HasProtoTraceContext[v30.SubscriptionResponse] =
    new HasProtoTraceContext[v30.SubscriptionResponse] {
      override def traceContext(value: v30.SubscriptionResponse) = value.traceContext
    }
}

abstract class ConsumesCancellableGrpcStreamObserver[
    E,
    R: HasProtoTraceContext,
] private[client] (
    context: CancellableContext,
    parentHasRunOnClosing: HasRunOnClosing,
    timeouts: ProcessingTimeout,
)(implicit executionContext: ExecutionContext)
    extends SequencerSubscription[E] {

  /** Stores ongoing work performed by `onNext` or `complete`. The contained future is completed
    * whenever these methods are not busy.
    */
  private val currentProcessing = new AtomicReference[Future[Unit]](Future.unit)
  private val currentAwaitOnNext = new AtomicReference[Promise[UnlessShutdown[Either[E, Unit]]]](
    Promise.successful(Outcome(Either.unit))
  )
  protected lazy val cancelledByClient = new AtomicBoolean(false)

  protected def callHandler: Traced[R] => EitherT[FutureUnlessShutdown, E, Unit]
  protected def onCompleteCloseReason: SubscriptionCloseReason[E]

  locally {
    import TraceContext.Implicits.Empty.*

    // Abort the current waiting when this observer is shut down
    runOnOrAfterClose_(new RunOnClosing {
      override def name: String = "cancel-current-await-in-onNext"
      override def done: Boolean = false
      override def run()(implicit traceContext: TraceContext): Unit =
        currentAwaitOnNext.get.trySuccess(AbortedDueToShutdown).discard
    })

    // Cancel the subscription when the parent is closed
    parentHasRunOnClosing.runOnOrAfterClose_(new RunOnClosing {
      override def name: String = "cancel-subscription-on-parent-close"
      override def done: Boolean = cancelledByClient.get() || isClosing
      override def run()(implicit traceContext: TraceContext): Unit = cancel()
    })
  }

  private def cancel(): Unit =
    if (!cancelledByClient.getAndSet(true)) context.close()

  private def appendToCurrentProcessing(
      next: Try[Unit] => Future[Unit]
  )(implicit traceContext: TraceContext): Unit = {
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
    import TraceContext.Implicits.Empty.*
    // Signal termination by client
    val completionF = Future(complete(SubscriptionCloseReason.Closed))
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
        timeouts.shutdownShort,
        onTimeout = onTimeout,
      ),
    )
  }

  private[client] val observer = new StreamObserver[R] {
    override def onNext(value: R): Unit = {
      // we take the unusual step of immediately trying to deserialize the trace-context
      // so it is available here for logging
      implicit val traceContext: TraceContext =
        SerializableTraceContext
          .fromProtoSafeV30Opt(noTracingLogger)(
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
            cancelableAwait.completeWith(callHandler(Traced(value)).value.unwrap)
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
      import TraceContext.Implicits.Empty.*
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
      import TraceContext.Implicits.Empty.*
      // Info level, as this occurs from time to time due to the invalidation of the authentication token.
      logger.info("The sequencer subscription has been terminated by the server.")
      complete(onCompleteCloseReason)
    }

  }
}

@VisibleForTesting
class GrpcSequencerSubscription[E, R: HasProtoTraceContext] private[transports] (
    context: CancellableContext,
    parentHasRunOnClosing: HasRunOnClosing,
    override protected val callHandler: Traced[R] => EitherT[FutureUnlessShutdown, E, Unit],
    override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ConsumesCancellableGrpcStreamObserver[E, R](
      context,
      parentHasRunOnClosing,
      timeouts,
    ) {

  override protected lazy val onCompleteCloseReason = GrpcSubscriptionError(
    GrpcError(
      "subscription",
      "sequencer",
      Status.UNAVAILABLE
        .withDescription("Connection terminated by the server.")
        .asRuntimeException(),
    )
  )

}

object GrpcSequencerSubscription {
  def fromSubscriptionResponse[E](
      context: CancellableContext,
      handler: SequencedEventHandler[E],
      hasRunOnClosing: HasRunOnClosing,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(protocolVersion: ProtocolVersion)(implicit
      executionContext: ExecutionContext
  ): ConsumesCancellableGrpcStreamObserver[E, v30.SubscriptionResponse] =
    new GrpcSequencerSubscription(
      context,
      hasRunOnClosing,
      deserializingSubscriptionHandler(
        handler,
        (value, traceContext) =>
          SubscriptionResponse.fromVersionedProtoV30(protocolVersion)(value)(traceContext),
      ),
      timeouts,
      loggerFactory,
    )

  private def deserializingSubscriptionHandler[E, R](
      handler: SequencedEventHandler[E],
      fromProto: (R, TraceContext) => ParsingResult[SubscriptionResponse],
  ): Traced[R] => EitherT[FutureUnlessShutdown, E, Unit] =
    withTraceContext { implicit traceContext => responseP =>
      EitherT(
        fromProto(responseP, traceContext)
          .fold(
            err =>
              FutureUnlessShutdown.failed(
                new RuntimeException(
                  s"Unable to parse response from sequencer. Discarding message. Reason: $err"
                )
              ),
            response => {
              handler(
                SequencedEventWithTraceContext(response.signedSequencedEvent)(response.traceContext)
              )
            },
          )
      )
    }
}
