// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerSubscriptionErrorGroup
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{Future, Promise}

/** Why did the sequencer subscription terminate */
sealed trait SubscriptionCloseReason[+E]

object SubscriptionCloseReason {

  final case class HandlerError[E](error: E) extends SubscriptionCloseReason[E]

  /** The handler threw an exception */
  final case class HandlerException(exception: Throwable) extends SubscriptionCloseReason[Nothing]

  /** The subscription itself failed.
    * [[transports.SequencerClientTransport]] implementations are expected to provide their own hierarchy of errors
    * and supply a matching [[SubscriptionErrorRetryPolicy]] to the [[SequencerClient]] for determining which
    * errors are appropriate for attempting to resume a subscription.
    */
  trait SubscriptionError extends SubscriptionCloseReason[Nothing]

  /** The subscription was denied
    * Implementations are expected to provide their own error of this type
    */
  trait PermissionDeniedError extends SubscriptionCloseReason[Nothing]

  /** The sequencer connection details are being updated, so the subscription is being closed so another one
    * is created with the updated transport.
    * This is not an error and also not a reason to close the sequencer client.
    */
  case object TransportChange extends SubscriptionCloseReason[Nothing]

  /** The subscription was closed by the client. */
  case object Closed extends SubscriptionCloseReason[Nothing]

  /** The subscription was closed due to an ongoing shutdown procedure. */
  case object Shutdown extends SubscriptionCloseReason[Nothing]
}

/** A running subscription to a sequencer.
  * Can be closed by the consumer or the producer.
  * Once closed the [[closeReason]] value will be fulfilled with the reason the subscription was closed.
  * Implementations are expected to immediately start their subscription unless otherwise stated.
  * If close is called while the handler is running closeReason should not be completed until the handler has completed.
  */
trait SequencerSubscription[HandlerError] extends FlagCloseableAsync with NamedLogging {

  protected val closeReasonPromise: Promise[SubscriptionCloseReason[HandlerError]] =
    Promise[SubscriptionCloseReason[HandlerError]]()

  /** Future which is completed when the subscription is closed.
    * If the subscription is closed in a healthy state the future will be completed successfully.
    * However if the subscription fails for an unexpected reason at runtime the completion should be failed.
    */
  val closeReason: Future[SubscriptionCloseReason[HandlerError]] = closeReasonPromise.future

  /** Completes the subscription with the given reason and closes it. */
  private[canton] def complete(reason: SubscriptionCloseReason[HandlerError])(implicit
      traceContext: TraceContext
  ): Unit

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

    Seq(
      AsyncCloseable(
        "sequencer-subscription", {
          closeReasonPromise.trySuccess(SubscriptionCloseReason.Closed).discard
          closeReasonPromise.future
        },
        timeouts.shutdownNetwork.duration,
      )
    )
  }

  // We don't want to throw here when closing the subscription fails (e.g in case of timeout)
  // If we threw we could short circuit the rest of the cleaning up of the gRPC stream and end up with
  // a stalled stream
  override def onCloseFailure(e: Throwable): Unit = {
    logger.warn("Failed to close sequencer subscription", e)(TraceContext.empty)
  }
}

object SequencerSubscriptionError extends SequencerSubscriptionErrorGroup {

  sealed trait SequencedEventError extends CantonError

  @Explanation(
    """This error indicates that a sequencer subscription to a recently onboarded sequencer attempted to read
      |an event replaced with a tombstone. A tombstone occurs if the timestamp associated with the event predates
      |the validity of the sequencer's signing key. This error results in the sequencer client disconnecting from
      |the sequencer."""
  )
  @Resolution(
    """Connect to another sequencer with older event history to consume the tombstoned events
      |before reconnecting to the recently onboarded sequencer."""
  )
  object TombstoneEncountered
      extends ErrorCode(
        id = "SEQUENCER_TOMBSTONE_ENCOUNTERED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with SequencedEventError
  }
}
