// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.TokenExpiration
import com.digitalasset.canton.sequencing.client.{SequencerSubscription, SubscriptionCloseReason}
import com.digitalasset.canton.sequencing.protocol.SubscriptionResponse
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.tracing.TraceContext.withTraceContext
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{FutureUtil, MaxBytesToDecompress, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.grpc.Context.CancellableContext
import io.grpc.Status.Code.CANCELLED
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}
import io.grpc.{Status, StatusRuntimeException}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.*
import scala.util.{Failure, Success, Try}

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
    Request,
    Response: HasProtoTraceContext,
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

  protected def callHandler: Traced[Response] => EitherT[FutureUnlessShutdown, E, Unit]
  protected def onCompleteCloseReason: SubscriptionCloseReason[E]
  protected def request: String
  protected def useManualFlowControl: Boolean

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

  @VisibleForTesting
  private[sequencing] val requestStream = new SingleUseCell[ClientCallStreamObserver[Request]]

  private[sequencing] val observer: ClientResponseObserver[Request, Response] =
    new ClientResponseObserver[Request, Response] {
      override def beforeStart(requestStream: ClientCallStreamObserver[Request]): Unit =
        if (useManualFlowControl) {
          requestStream.disableAutoRequestWithInitial(1)
          ConsumesCancellableGrpcStreamObserver.this.requestStream
            .putIfAbsent(requestStream)
            .foreach(_ => throw new IllegalStateException("beforeStart called multiple times"))
        }

      override def onNext(value: Response): Unit = {
        // we take the unusual step of immediately trying to deserialize the trace-context
        // so it is available here for logging
        implicit val traceContext: TraceContext =
          SerializableTraceContext
            .fromProtoSafeV30Opt(noTracingLogger)(
              implicitly[HasProtoTraceContext[Response]].traceContext(value)
            )
            .unwrap

        logger.debug("Received a message from the sequencer.")

        val current = Promise[Unit]()
        val closeReasonOO = synchronizeWithClosingF(functionFullName) {
          val handlerResult = for {
            _ <- Future.fromTry(Try(appendToCurrentProcessing(_ => current.future)))
            cancelableAwait = Promise[UnlessShutdown[Either[E, Unit]]]()
            _ = currentAwaitOnNext.set(cancelableAwait)
            _ = cancelableAwait.completeWith(
              Future.delegate(callHandler(Traced(value)).value.unwrap)
            )
            result <- cancelableAwait.future
          } yield result

          // as we're responsible for calling the handler we block onNext from processing further items
          // calls to onNext are guaranteed to happen in order

          handlerResult
            .transform[Option[SubscriptionCloseReason[E]]](
              (tryUSE: Try[UnlessShutdown[Either[E, Unit]]]) =>
                tryUSE match {
                  case Failure(ex) => Success(Some(SubscriptionCloseReason.HandlerException(ex)))
                  case Success(Outcome(Left(err))) =>
                    Success(Some(SubscriptionCloseReason.HandlerError(err)))
                  case Success(Outcome(Right(_))) =>
                    // we'll continue
                    Success(None)
                  case Success(AbortedDueToShutdown) =>
                    Success(Some(SubscriptionCloseReason.Shutdown))
                }
            )
        }.map { result =>
          result.foreach(complete)
          logger.debug("Finished processing of the sequencer message.")
          result
        }.onShutdown {
          logger.debug(s"The message is not processed, as the node is closing.")
          Some(AbortedDueToShutdown)
        }.thereafter(_ => current.success(()))

        if (useManualFlowControl) {
          FutureUtil.doNotAwait(
            closeReasonOO.thereafter {
              case Success(None) => requestStream.get.foreach(_.request(1))
              case Success(Some(_)) | Failure(_) =>
              // An error was signaled with `Success(Some(_))` or the processing failed completely (Failure(_)),
              // therefore we don't request more data, as the entire request is going to be cancelled/closed anyway.
            },
            s"Processing of $value",
          )
        } else {
          timeouts.unbounded
            .await(s"${this.getClass}: Blocking processing of further items")(closeReasonOO)
            .discard
        }
      }

      override def onError(t: Throwable): Unit = {
        import TraceContext.Implicits.Empty.*
        FutureUtil.doNotAwait(
          // re-sync on onNext processing but proceed even if the handler threw an exception
          currentProcessing
            .get()
            // onNext failures are already logged. We only want to log issues with the thereafter.
            // therefore, remapping any prior failures to unit here.
            .recover(_ => ())
            .thereafter { _ =>
              t match {
                case s: StatusRuntimeException if s.getStatus.getCode == CANCELLED =>
                  if (cancelledByClient.get()) {
                    logger.info(
                      "gRPC subscription successfully closed due to client shutdown.",
                      s.getStatus.getCause,
                    )
                    complete(SubscriptionCloseReason.Closed)
                  } else {
                    // As the client has not cancelled the subscription, the problem must be on the server side.
                    val grpcError =
                      GrpcServiceUnavailable(
                        request,
                        "sequencer",
                        s.getStatus,
                        Option(s.getTrailers),
                        None,
                      )
                    complete(GrpcSubscriptionError(grpcError))
                  }
                case s: StatusRuntimeException =>
                  val grpcError = GrpcError(request, "sequencer", s)
                  if (
                    s.getStatus.getCode == Status.Code.UNAVAILABLE &&
                    s.getStatus.getDescription == ServerSubscriptionCloseReason.TokenExpired.description
                  ) {
                    logger.info(
                      "The sequencer subscription has been terminated by the server due to a token expiration."
                    )
                    complete(TokenExpiration)
                  } else if (s.getStatus.getCode == Status.Code.PERMISSION_DENIED) {
                    complete(GrpcPermissionDeniedError(grpcError))
                  } else {
                    complete(GrpcSubscriptionError(grpcError))
                  }
                case exception: Throwable =>
                  logger.error("The sequencer subscription failed unexpectedly.", t)
                  complete(GrpcSubscriptionUnexpectedException(exception))
              }
            },
          "on-error-after-processing",
        )

      }

      override def onCompleted(): Unit = {
        import TraceContext.Implicits.Empty.*
        FutureUtil.doNotAwait(
          // re-sync on onNext processing but proceed even if the handler threw an exception
          currentProcessing.get().thereafter { _ =>
            // Info level, as this occurs from time to time when a member is disconnected.
            logger.info("The sequencer subscription has been terminated by the server.")
            complete(onCompleteCloseReason)
          },
          "on-completed-after-processing",
        )
      }

    }
}

/** Reasons for closing a subscription on the server side. */
object ServerSubscriptionCloseReason {

  /** Transient reasons associated to an UNAVAILABLE status. */
  sealed trait TransientCloseReason {
    def description: String
  }

  case object TokenExpired extends TransientCloseReason {
    override def description: String = "Subscription token has expired"
  }

  case object TooManySubscriptions extends TransientCloseReason {
    override def description: String = "Too many open subscriptions for member"
  }

}

@VisibleForTesting
class GrpcSequencerSubscription[E, Request, Response: HasProtoTraceContext] private[transports] (
    context: CancellableContext,
    parentHasRunOnClosing: HasRunOnClosing,
    override protected val callHandler: Traced[Response] => EitherT[FutureUnlessShutdown, E, Unit],
    override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ConsumesCancellableGrpcStreamObserver[E, Request, Response](
      context,
      parentHasRunOnClosing,
      timeouts,
    ) {
  override protected def useManualFlowControl: Boolean = true

  override protected lazy val onCompleteCloseReason = GrpcSubscriptionError(
    GrpcError(
      request,
      "sequencer",
      Status.UNAVAILABLE
        .withDescription("Connection terminated by the server.")
        .asRuntimeException(),
    )
  )

  override protected def request: String = "subscription"
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
  ): ConsumesCancellableGrpcStreamObserver[E, v30.SubscriptionRequest, v30.SubscriptionResponse] =
    new GrpcSequencerSubscription[E, v30.SubscriptionRequest, v30.SubscriptionResponse](
      context,
      hasRunOnClosing,
      deserializingSubscriptionHandler(
        handler,
        (value, traceContext) => {
          SubscriptionResponse
            .fromVersionedProtoV30(MaxBytesToDecompress.HardcodedDefault, protocolVersion)(value)(
              traceContext
            )
        },
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
