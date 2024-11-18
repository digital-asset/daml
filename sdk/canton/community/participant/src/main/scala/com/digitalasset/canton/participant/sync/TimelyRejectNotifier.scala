// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Monad
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.store.ParticipantNodeEphemeralState
import com.digitalasset.canton.participant.sync
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class TimelyRejectNotifier(
    rejecter: TimelyRejectNotifier.TimelyRejecter,
    initialUpperBound: CantonTimestamp,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import TimelyRejectNotifier.*

  /** A non-strict upper bound on the timestamps with which the `rejecter` has been notified.
    * Also stores the internal state of the notification state machine so that they can be updated atomically.
    */
  private val upperBoundOnNotification: AtomicReference[(CantonTimestamp, UnlessShutdown[State])] =
    new AtomicReference[(CantonTimestamp, UnlessShutdown[State])](
      (initialUpperBound, Outcome(Idle))
    )

  /** Notifies the `rejecter` that the clean sequencer index
    * has advanced to the given point. Does nothing if a notification with a higher timestamp has
    * already happened or is happening concurrently.
    *
    * The method returns immediately after the notification has been scheduled.
    * The notification itself happens asynchronously in a spawned future.
    */
  def notifyAsync(observedSequencerTime: Traced[CantonTimestamp]): Unit =
    notifyLoop(observedSequencerTime.value, increaseBound = true)(
      observedSequencerTime.traceContext
    ).discard

  /** Notifies the `rejecter` again
    * if it may have already been notified for the given timestamp or later.
    * Does nothing if the `rejecter` has not yet been notified of the given timestamp or any later timestamp.
    *
    * The method returns immediately after the notification has been scheduled.
    * The notification itself happens asynchronously in a spawned future.
    *
    * When a timely rejection's timestamp has been back-dated to the sequencing timestamp of an already processed message,
    * a call to this method ensures that the rejection will be emitted even if no further messages are processed from the domain.
    */
  def notifyIfInPastAsync(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Boolean =
    notifyLoop(timestamp, increaseBound = false)

  /** Schedules a notification up to the given bound if one of the following holds:
    * - The current [[upperBoundOnNotification]] is lower than `bound` and `increaseBound` is true
    * - The current [[upperBoundOnNotification]] is at least `bound` and `increaseBound` is false
    *
    * Increases [[upperBoundOnNotification]] to `bound` if `increaseBound` is true.
    *
    * @return whether a notification was scheduled
    */
  private def notifyLoop(bound: CantonTimestamp, increaseBound: Boolean)(implicit
      traceContext: TraceContext
  ): Boolean = {
    // First advance the upper bound, then notify, to make sure that the upper bound really is an upper bound.
    val (oldBound, oldState) = upperBoundOnNotification.getAndUpdate { case (oldBound, oldState) =>
      val newBound = if (increaseBound) oldBound max bound else oldBound
      val shouldNotify = (oldBound < bound) == increaseBound
      val newState = oldState.map {
        case Idle => if (shouldNotify) Running else Idle
        case Running => if (shouldNotify) Pending(traceContext) else Running
        case pending @ Pending(_) =>
          // Update the trace context only if we increase the bound
          if (increaseBound && oldBound < bound) Pending(traceContext)
          else pending
      }
      newBound -> newState
    }
    val shouldNotify = (oldBound < bound) == increaseBound
    oldState match {
      case Outcome(Idle) if shouldNotify =>
        val notifiedF =
          Monad[Future].tailRecM(LoopState(bound, increaseBound, traceContext))(doNotify)
        FutureUtil.doNotAwait(notifiedF, "Timely reject notification failed")
        true
      case Outcome(_) =>
        // Nothing to do as there is already another future notifying the in-flight submission tracker
        // and this future will pick up the updated state when it's done.
        shouldNotify
      case AbortedDueToShutdown =>
        logger.debug(s"Aborted timely rejects upto $bound due to shutdown")
        false
    }
  }

  /** Notifies the in-flight submission tracker about the timestamp in the loop state,
    * then updates the notification state again.
    * Returns [[scala.Left$]] if another notification should be run immediately after.
    */
  private def doNotify(loopState: LoopState): Future[Either[LoopState, Unit]] = {
    val theBound = loopState.newBound
    implicit val traceContext: TraceContext = loopState.traceContext

    val notifyF =
      if (loopState.boundIncreased) rejecter.notify(theBound) else rejecter.notifyAgain(theBound)
    notifyF.unwrap
      .recover { case ex =>
        // Merely log the exception and keep going as a later notification may still succeed.
        logger.error(
          s"Notifying the in-flight submission tracker for $theBound failed",
          ex,
        )
        UnlessShutdown.unit
      }
      .map { notificationOutcome =>
        // Finally update the state and check whether we need to notify once more
        val (bound, oldState) = upperBoundOnNotification.getAndUpdate { case (bound, state) =>
          val newState = state match {
            case Outcome(Running) =>
              notificationOutcome.map((_: Unit) => Idle)
            case Outcome(Pending(_)) =>
              notificationOutcome.map((_: Unit) => Running)
            case Outcome(Idle) | AbortedDueToShutdown =>
              ErrorUtil.internalError(
                new ConcurrentModificationException(
                  s"Internal state of TimelyRejectNotifier changed concurrently to $state"
                )
              )
          }
          (bound, newState)
        }
        oldState match {
          case Outcome(Running) => Either.unit
          case Outcome(Pending(newTraceContext)) =>
            Either.cond(
              !notificationOutcome.isOutcome,
              (), {
                val boundIncreased = bound > theBound
                LoopState(bound, boundIncreased, newTraceContext)
              },
            )
          case _ =>
            ErrorUtil.invalidState("getAndUpdate should already have thrown an exception")
        }
      }
  }
}

object TimelyRejectNotifier {

  def apply(
      participantNodeEphemeralState: ParticipantNodeEphemeralState,
      domainId: DomainId,
      initialUpperBound: CantonTimestamp,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): TimelyRejectNotifier = {
    class InFlightSubmissionTimelyRejecter extends TimelyRejecter {
      override def notify(
          upToInclusive: CantonTimestamp
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        participantNodeEphemeralState.inFlightSubmissionTracker
          .timelyReject(
            domainId,
            upToInclusive,
            participantNodeEphemeralState.participantEventPublisher,
          )
          .valueOr { case InFlightSubmissionTracker.UnknownDomain(domainId) =>
            // The CantonSyncService removes the SyncDomain from the connected domains map
            // before the SyncDomain is closed. So guarding the timely rejections against the SyncDomain being closed
            // cannot eliminate this possibility.
            //
            // It is safe to skip the timely rejects because crash recovery and replay will take care
            // upon the next reconnection.
            loggerFactory
              .getLogger(classOf[sync.TimelyRejectNotifier])
              .info(
                s"Skipping timely rejects for domain $domainId upto $upToInclusive because domain is being disconnected."
              )
          }

      override def notifyAgain(upToInclusive: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] =
        participantNodeEphemeralState.inFlightSubmissionTracker.timelyRejectAgain(
          domainId,
          upToInclusive,
          participantNodeEphemeralState.participantEventPublisher,
        )
    }
    val rejecter = new InFlightSubmissionTimelyRejecter
    new TimelyRejectNotifier(rejecter, initialUpperBound, loggerFactory)
  }

  trait TimelyRejecter {
    def notify(upToInclusive: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]

    /** May be called only if an earlier call to `notify` with the same or a later timestamp has already completed. */
    def notifyAgain(upToInclusive: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]
  }

  /** The states of the notification state machine implemented by [[TimelyRejectNotifier.notifyLoop]]
    * and [[TimelyRejectNotifier.doNotify]].
    * - In [[Idle]], nothing is happening.
    * - In [[Running]], a single notification is currently running.
    * - In [[Pending]], a single notification is currently running and a follow-up notification should be scheduled.
    *   When the currently running notification finishes, it will initiate another notification with the bound
    *   in [[TimelyRejectNotifier.upperBoundOnNotification]] at that time.
    * - If a notification results in [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]],
    *   no further notifications will happen.
    *
    * In the diagram below, `b` refers to the bound in [[TimelyRejectNotifier.upperBoundOnNotification]].
    * <pre>
    *   ┌──┐ notify if ts<=b           ┌─────┐ notify if ts<=b
    *   │  │ notifyInPast if ts>b      │     │ notifyInPast if ts>b
    *   │  │                           │     │
    *   │  │   notify if ts>b          │     │   notify if ts>b
    * ┌─┴──▼─┐ notifyInPast if ts<=b ┌─┴─────▼─┐ notifyInPast if ts<=b ┌─────────┐
    * │      ├───────────────────────►         ├───────────────────────►         ├───────┐
    * │ Idle │                       │ Running │                       │ Pending │       │ notify
    * │      │                       │         │                       │         │       │ notifyInPast
    * │      ◄───────────────────────┤         ◄───────────────────────┤         ◄───────┘
    * └──────┘    done notifying     └────┬────┘   done notifying      └────┬────┘
    *                                     │                                 │
    *                                     │                                 │onShutdown
    *                                     │                                 │
    *                                     │                                 │
    *                                     │      onShutdown            ┌────▼─────┐
    *                                     └────────────────────────────► Aborted  │
    *                                                                  │ DueTo    │
    *                                                                  │ Shutdown │
    *                                                                  │          │
    *                                                                  └──────────┘
    * </pre>
    */
  private sealed trait State extends Product with Serializable
  private case object Idle extends State
  private case object Running extends State
  private final case class Pending(traceContext: TraceContext) extends State

  private final case class LoopState(
      newBound: CantonTimestamp,
      boundIncreased: Boolean,
      traceContext: TraceContext,
  )
}
