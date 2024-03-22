// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.SequencedEventError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.ShowUtil.*
import io.grpc.Status
import io.grpc.stub.ServerCallStreamObserver

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Trait for the grpc managed subscription to allow easy testing without GRPC infrastructure */
trait ManagedSubscription extends FlagCloseable with CloseNotification {

  /** If and when the subscription is due to be expired.
    * Should be set if using sequencer authentication with expiring tokens.
    * Will be unset if authentication is not used and can be left running indefinitely.
    */
  val expireAt: Option[CantonTimestamp]
}

/** Creates and manages a SequencerSubscription for the given grpc response observer.
  * The sequencer subscription could be closed internally due to errors on event publishing
  * or when the grpc connection is cancelled, in these cases [[closedCallback]] will be called
  * to allow external users to perform any administrative tasks.
  * Any exception thrown by the call to `observer.onNext` will cause the subscription to close.
  */
private[service] class GrpcManagedSubscription[T](
    createSubscription: SerializedEventOrErrorHandler[SequencedEventError] => EitherT[
      Future,
      CreateSubscriptionError,
      SequencerSubscription[SequencedEventError],
    ],
    observer: ServerCallStreamObserver[T],
    val member: Member,
    val expireAt: Option[CantonTimestamp],
    override protected val timeouts: ProcessingTimeout,
    baseLoggerFactory: NamedLoggerFactory,
    toSubscriptionResponse: OrdinarySerializedEvent => T,
)(implicit ec: ExecutionContext)
    extends ManagedSubscription
    with NamedLogging {

  protected val loggerFactory: NamedLoggerFactory =
    baseLoggerFactory.append("member", show"$member")
  private val subscriptionRef =
    new AtomicReference[Option[SequencerSubscription[SequencedEventError]]](None)

  /** How should the response observer be closed
    */
  private sealed trait ObserverCloseSignal
  private object NoSignal extends ObserverCloseSignal
  private object CompleteSignal extends ObserverCloseSignal
  private case class ErrorSignal(cause: Throwable) extends ObserverCloseSignal

  private val closeSignalRef = new AtomicReference[Option[ObserverCloseSignal]](None)

  private def setCloseSignal(signal: ObserverCloseSignal): Unit = {
    val _ = closeSignalRef.compareAndSet(None, Some(signal))
  }

  // sets the observer signal value and closes this managed subscription.
  // take care not to call this from a performUnlessClosing block as that will likely cause a deadlock.
  private def signalAndClose(signal: ObserverCloseSignal): Unit = {
    setCloseSignal(signal)
    close()
  }

  // if the underlying grpc call is cancelled then close the subscription
  // as the underlying channel is cancelled we can no longer send a response
  observer.setOnCancelHandler(() => signalAndClose(NoSignal))

  private val handler: SerializedEventOrErrorHandler[SequencedEventError] = {
    case Right(event) =>
      implicit val traceContext: TraceContext = event.traceContext
      Future {
        Right(performUnlessClosing("grpc-managed-subscription-handler") {
          observer.onNext(toSubscriptionResponse(event))
        }.onShutdown(()))
      }.recover { case NonFatal(e) =>
        logger.warn(
          "Unexpected error was thrown while publishing a sequencer event to GRPC subscriber",
          e,
        )
        signalAndClose(ErrorSignal(Status.INTERNAL.withCause(e).asException()))
        Right(())
      }
    case Left(error) =>
      // Turn a subscription error (e.g. due to a tombstone) into a grpc observer error and
      // terminate the subscription (rather than extending the SequencerResponse with
      // tombstone related information).
      Future {
        // Close asynchronously to avoid deadlocking with the DirectSequencerSubscription's
        //  "done" pekko flow that invokes this handler.
        signalAndClose(ErrorSignal(error.asGrpcError))
      }.discard
      Future.successful(Left(error))
  }

  // TODO(#5705) Redo this when revisiting the subscription pool
  withNewTraceContext { implicit traceContext =>
    val shouldClose = performUnlessClosing("grpc-managed-subscription-handler") {
      val createSub = Try({
        val subscription = createSubscription(handler)
        timeouts.unbounded.await(s"Creation of subscription handler")(subscription.value)
      })
      createSub match {
        case Failure(exception) =>
          logger.warn("Creating sequencer subscription failed", exception)
          setCloseSignal(ErrorSignal(exception))
          true
        case Success(Left(err)) =>
          logger.warn(s"Creating sequencer subscription returned error: $err")
          setCloseSignal(
            ErrorSignal(Status.FAILED_PRECONDITION.withDescription(err.toString).asException())
          )
          true
        case Success(Right(subscription)) =>
          subscriptionRef.set(Some(subscription))
          logger.debug(
            "Underlying subscription has been successfully created (may still be starting)"
          )
          false
      }
    } onShutdown false

    // if we have set a signal value then immediately close the subscription
    if (shouldClose) close()
  }

  /** Close the subscription.
    */
  override def onClosed(): Unit = withNewTraceContext { implicit traceContext =>
    try {
      // if a signal hasn't been set, then we'll assume we were just closed
      val closeSignal = closeSignalRef.get().getOrElse(CompleteSignal)

      // close subscription if set
      logger.debug(
        s"Closing subscription for $member and completing subscription observer with $closeSignal"
      )

      subscriptionRef
        .get()
        .fold(logger.debug("Closing but underlying subscription has not been created"))(_.close())

      closeSignal match {
        case NoSignal =>
          () // don't send anything, likely as the underlying channel is already cancelled
        case CompleteSignal => observer.onCompleted()
        case ErrorSignal(cause) => observer.onError(cause)
      }
    } finally notifyClosed()
  }

}
