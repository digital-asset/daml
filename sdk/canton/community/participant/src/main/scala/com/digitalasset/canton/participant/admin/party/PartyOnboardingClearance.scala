// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{PartyToParticipant, TopologyChangeOp}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Holds all necessary information to perform an onboarding clearance.
  */
private[admin] final case class OnboardingClearanceTask(
    partyId: PartyId,
    earliestClearanceTime: CantonTimestamp,
    onboardingEffectiveAt: CantonTimestamp,
)

/** Schedules and executes party onboarding clearance tasks.
  *
  * This implementation is thread-safe and idempotent. It ensures that for any given party on a
  * specific participant and synchronizer, only one clearance task is scheduled at a time.
  *
  * The clearance process is passive:
  *   1. A task is submitted and held in a map.
  *   1. A time-based trigger is scheduled to *propose* the clearance transaction.
  *   1. The `observed` method is responsible for detecting the effective transaction and removing
  *      the task.
  *
  * Note: Non-final class required for mocking.
  */
class OnboardingClearanceScheduler(
    participantId: ParticipantId,
    psid: PhysicalSynchronizerId,
    getConnectedSynchronizer: () => Option[ConnectedSynchronizer],
    protected val loggerFactory: NamedLoggerFactory,
    topologyWorkflow: PartyReplicationTopologyWorkflow,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with NamedLogging {

  @VisibleForTesting
  private[party] val pendingClearances: TrieMap[PartyId, OnboardingClearanceTask] = TrieMap.empty

  /** Attempts to clear the party's onboarding flag.
    *
    * If the flag isn't cleared immediately, schedules a deferred `OnboardingClearanceTask` via
    * `schedule()`.
    *
    * @param partyId
    *   The party whose onboarding flag to clear.
    * @param onboardingEffectiveAt
    *   Effective time of the original transaction that set the `onboarding = true` flag.
    * @return
    *   A `FutureUnlessShutdown` containing:
    *   - `Right(FlagNotSet)` if flag is/was already clear.
    *   - `Right(FlagSet(safeTime))` if flag is set; task scheduled for `safeTime`.
    *   - `Left(String)` on precondition violation or check error.
    *
    * @note
    *   Preconditions:
    *   1. Connection: `getConnectedSynchronizer()` must return `Some(...)`.
    *   1. PSId: Connected synchronizer's `psid` must match this scheduler's `psid`.
    *   1. Timestamp: `onboardingEffectiveAt` must be the *exact* effective time of the original
    *      `onboarding = true` transaction.
    *   1. Idempotency: Relies on `schedule()` to ensure only one task is scheduled per party.
    */
  private[admin] def requestClearance(
      partyId: PartyId,
      onboardingEffectiveAt: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyOnboardingFlagStatus] =
    for {
      connectedSynchronizer <- EitherT.fromOption[FutureUnlessShutdown](
        getConnectedSynchronizer(),
        s"Synchronizer connection is not ready (absent): Onboarding flag clearance request for $psid (party: $partyId, participant: $participantId).",
      )

      _ <- EitherT.cond[FutureUnlessShutdown](
        connectedSynchronizer.psid == psid,
        (),
        s"PSId mismatch: Expected $psid, got ${connectedSynchronizer.psid} for onboarding flag clearance request (party: $partyId, participant: $participantId).",
      )

      // Attempt to clear the onboarding flag as initial step
      outcome <- topologyWorkflow.authorizeClearingOnboardingFlag(
        partyId,
        targetParticipantId = participantId,
        EffectiveTime(onboardingEffectiveAt),
        connectedSynchronizer,
      )

      _ = outcome match {
        case FlagNotSet =>
          logger.info(
            s"No onboarding flag clearance. Flag is not set for party $partyId on participant $participantId (physical synchronizer: $psid)."
          )

        case FlagSet(safeToClear) =>
          logger.info(
            s"Deferring onboarding flag clearanceTask until $safeToClear for party $partyId until $safeToClear (participant: $participantId, physical synchronizer: $psid)."
          )
          val clearanceTask = OnboardingClearanceTask(
            partyId = partyId,
            earliestClearanceTime = safeToClear,
            onboardingEffectiveAt = onboardingEffectiveAt,
          )
          schedule(clearanceTask)
      }

    } yield outcome

  private def schedule(
      task: OnboardingClearanceTask
  )(implicit traceContext: TraceContext): Unit = {
    val oldValueO = pendingClearances.putIfAbsent(task.partyId, task)

    // Only schedule the trigger if the current invocation *put* the task (oldValueO is None)
    if (oldValueO.isEmpty) {
      logger.info(s"Scheduled a new onboarding clearance task for party ${task.partyId}.")
      scheduleTrigger(task)
    } else {
      logger.info(
        s"An existing onboarding clearance task was found for party ${task.partyId}. Not scheduling a new one."
      )
    }
  }

  /** Schedules a time-based trigger to attempt the clearance. */
  private def scheduleTrigger(
      task: OnboardingClearanceTask
  )(implicit traceContext: TraceContext): Unit =
    getConnectedSynchronizer() match {
      case Some(connectedSynchronizer) =>
        val triggerTime = task.earliestClearanceTime.immediateSuccessor
        val tickFutureO = connectedSynchronizer.ephemeral.timeTracker.awaitTick(triggerTime)

        tickFutureO match {
          case Some(tickFuture) =>
            tickFuture.onComplete {
              case Success(_) =>
                // Time has come, attempt to trigger the clearance
                triggerClearanceAttempt(task)
              // TODO(#29427) – Add a re-try for retriable errors
              case Failure(e) =>
                logger.warn(
                  s"Failed to await tick for onboarding clearance task (party: ${task.partyId}).",
                  e,
                )
            }
          case None =>
            logger.info(
              s"Time $triggerTime for onboarding clearance task for party ${task.partyId} is already in the past. Triggering immediately."
            )
            triggerClearanceAttempt(task)
        }
      case None =>
        logger.error(
          s"Cannot schedule trigger for party ${task.partyId}: connection to $psid is not ready (absent)."
        )
    }

  /** This method is called once the `earliestClearanceTime` for a task has been reached. It makes a
    * single attempt to propose the clearance transaction. It does *not* remove the task; removal is
    * handled by `observed`.
    */
  private def triggerClearanceAttempt(
      task: OnboardingClearanceTask
  )(implicit traceContext: TraceContext): Unit =
    // Check if the task is still pending. It might have been cleared by `observed`
    // between when `schedule` was called and when this trigger fired.
    if (pendingClearances.get(task.partyId).contains(task)) {
      getConnectedSynchronizer() match {
        case Some(connectedSynchronizer) =>
          if (connectedSynchronizer.psid == psid) {
            logger.info(
              s"Safe time for party ${task.partyId} reached. Proposing topology transaction to clear onboarding flag."
            )
            val clearanceF = topologyWorkflow
              .authorizeClearingOnboardingFlag(
                task.partyId,
                targetParticipantId = participantId,
                EffectiveTime(task.onboardingEffectiveAt),
                connectedSynchronizer,
              )
              .value
              .unwrap

            clearanceF.onComplete {
              case Success(UnlessShutdown.AbortedDueToShutdown) =>
                logger.info(
                  s"Onboarding flag clearance proposal for ${task.partyId} was aborted due to shutdown."
                )
              case Failure(e) =>
                logger.error(
                  s"Onboarding flag clearance proposal for ${task.partyId} failed with an exception.",
                  e,
                )
              case Success(UnlessShutdown.Outcome(result)) =>
                result match {
                  case Right(FlagNotSet) =>
                    logger.info(
                      s"Onboarding flag for party ${task.partyId} was already clear or cleared immediately upon proposal."
                    )
                  case Right(FlagSet(_)) =>
                    logger.info(
                      s"Onboarding flag clearance proposal for party ${task.partyId} submitted. Waiting for it to become effective."
                    )
                  case Left(error) =>
                    logger.error(
                      s"Onboarding flag clearance proposal for party ${task.partyId} failed: $error. Task will remain pending for manual retry."
                    )
                }
            }
          } else {
            logger.error(
              s"Cannot trigger clearance for party ${task.partyId}: PSId mismatch on trigger. Expected $psid, got ${connectedSynchronizer.psid}. Task will remain pending for manual retry."
            )
          }
        case None =>
          logger.error(
            s"Cannot trigger clearance for party ${task.partyId}: connection to $psid is not ready (absent)."
          )
      }
    } else {
      // Likely cause for the task already being removed is a call to the 'observed' method
      logger.info(
        s"Onboarding flag clearance trigger for party ${task.partyId} fired, but task was already removed."
      )
    }

  /** This must be called whenever a topology transaction is committed. It inspects transactions as
    * they become effective and removes any matching tasks from `pendingClearances`.
    *
    * This method must be **idempotent**, as it may be re-called with the same transactions during
    * crash recovery. The current implementation is safe because removing items from a `TrieMap`
    * (`--=`) is an idempotent operation.
    */
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.delegate {
      if (pendingClearances.nonEmpty) {
        // Find all parties that are either cleared or removed
        val partiesToRemove = (for {
          signedTx <- transactions.iterator if !signedTx.isProposal
          ptp <- signedTx.transaction.mapping.select[PartyToParticipant]

          op = signedTx.transaction.operation
          hosting = ptp.participants.find(_.participantId == this.participantId)

          // Case 1: Clearance (Replace op, participant is host, onboarding is false)
          isClearance = op == TopologyChangeOp.Replace && hosting.exists(!_.onboarding)

          // Case 2: Offboarding (Remove op, participant was a host)
          isOffboarding = op == TopologyChangeOp.Remove && hosting.isDefined
          if isClearance || isOffboarding
        } yield {
          if (pendingClearances.contains(ptp.partyId)) {
            val reason = if (isClearance) "clearance" else "offboarding"
            logger.info(
              s"Detected effective party $reason for party ${ptp.partyId} (at $effectiveTimestamp). Removing pending clearance task."
            )
          }

          ptp.partyId
        }).toSet

        pendingClearances --= partiesToRemove
      }

      FutureUnlessShutdown.unit
    }
}
