// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.CommitmentSnapshot
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.{concurrent, mutable}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class AcsCommitmentMultiHostedPartyTracker(
    participantId: ParticipantId,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {

  private[pruning] val commitmentThresholdsMap
      : concurrent.Map[CommitmentPeriod, Set[MultiHostedPartyTracker]] =
    new ConcurrentHashMap[CommitmentPeriod, Set[MultiHostedPartyTracker]]().asScala

  /** This builds a new period based on given snapshots and adds the new counterParticipant mapping
    * to its own tracked map. It behaves idempotent and does not overwrite (in case method is called
    * twice, but newCommit is called in between). It can handle an empty snapshots. It does not
    * validate that the period and snapshots correlates.
    */
  def trackPeriod(
      period: CommitmentPeriod,
      topoSnapshot: TopologySnapshot,
      acsSnapshot: CommitmentSnapshot,
      noWaitParticipants: Set[ParticipantId],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(s"Tracking $period for multi-hosted parties")
    if (commitmentThresholdsMap.contains(period))
      FutureUnlessShutdown.unit
    else {
      for {
        _ <- FutureUnlessShutdown.unit
        activeContracts = acsSnapshot.active
        partyParticipantMap <- topoSnapshot.activeParticipantsOfPartiesWithInfo(
          activeContracts.keys.flatten.toSeq
        )

        newValues = partyParticipantMap
          .map { case (partyId, partyInfo) =>
            val participantsExcludingLocal = partyInfo.participants.removed(participantId)
            val participantsExcludingNoWaits = participantsExcludingLocal.filterNot { case (p, _) =>
              noWaitParticipants.contains(p)
            }
            if (participantsExcludingLocal.isEmpty)
              None
            else
              Some(
                new MultiHostedPartyTracker(
                  partyId,
                  new AtomicInteger(
                    // We take the threshold value -1 (if local participant is contained within the set).
                    // We use math min for the case where a majority of the parties other participants is on the no wait list,
                    // for example if p1 is hosted on r1,r2,r3 with a threshold of 3, but r2 & r3 are on the no wait,
                    // then the threshold should be treated as 2.
                    math.min(
                      partyInfo.threshold.value - (partyInfo.participants.size - participantsExcludingLocal.size),
                      participantsExcludingNoWaits.size,
                    )
                  ),
                  mutable.Set(participantsExcludingNoWaits.keys.toSeq*),
                )
              )
          }
          .collect { case Some(tracker) if (tracker.threshold.get() > 0) => tracker }
          .toSet
        _ = if (newValues.nonEmpty) commitmentThresholdsMap.putIfAbsent(period, newValues)
      } yield ()
    }
  }

  /** takes a sender and Non-Empty set of periods and updates the internal Map it returns a set of
    * ([[com.digitalasset.canton.protocol.messages.CommitmentPeriod]],[[TrackedPeriodState]])
    * telling if the period is [[TrackedPeriodState.Cleared]], [[TrackedPeriodState.Outstanding]] or
    * [[TrackedPeriodState.NotTracked]]
    *
    * it is up to the caller to ensure that the stores get properly updated.
    */
  def newCommit(
      sender: ParticipantId,
      periods: NonEmpty[Set[CommitmentPeriod]],
  )(implicit traceContext: TraceContext): NonEmpty[Set[(CommitmentPeriod, TrackedPeriodState)]] =
    periods.map { period =>
      val _ = commitmentThresholdsMap.updateWith(period) {
        case Some(trackerSet) =>
          Some(trackerSet.flatMap(tracker => tracker.reduce(sender)))
        case None => None
      }

      commitmentThresholdsMap.get(period) match {
        case Some(list) =>
          if (list.isEmpty) {
            val _ = commitmentThresholdsMap.remove(period)
            logger.debug(s"commitment from $sender cleared $period")

            (period, TrackedPeriodState.Cleared)
          } else {
            logger.debug(
              s"commitment from $sender reduced thresholds $period"
            )

            (period, TrackedPeriodState.Outstanding)
          }
        case None => (period, TrackedPeriodState.NotTracked)
      }
    }
  def pruneOldPeriods(until: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"received pruning with timestamp $until")
    commitmentThresholdsMap.foreach { case (period, _) =>
      if (period.toInclusive < until) {
        val _ = commitmentThresholdsMap.remove(period)
      }
      ()
    }
  }
}

private[pruning] class MultiHostedPartyTracker(
    val partyId: LfPartyId,
    val threshold: AtomicInteger,
    val missingParticipants: mutable.Set[ParticipantId],
) extends PrettyPrinting {

  /** removes a given participantId from the missingParticipants Set. reduces threshold if
    * participantId is present. giving an invalid or repeated participantId has no effect.
    */
  def reduce(participant: ParticipantId): Option[MultiHostedPartyTracker] =
    if (missingParticipants.remove(participant)) {
      val newValue = threshold.addAndGet(-1)
      if (newValue <= 0)
        None
      else
        Some(this)
    } else
      Some(this)

  override protected def pretty: Pretty[MultiHostedPartyTracker] =
    prettyOfClass(
      param("party", _.partyId),
      param("threshold", _.threshold.get()),
      param("missingCounterParticipant", _.missingParticipants.toSeq),
    )

}

sealed trait TrackedPeriodState {}

object TrackedPeriodState {
  object Cleared extends TrackedPeriodState

  object Outstanding extends TrackedPeriodState

  object NotTracked extends TrackedPeriodState
}
