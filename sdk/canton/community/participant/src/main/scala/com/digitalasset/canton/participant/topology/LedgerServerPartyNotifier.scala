// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{
  FutureUnlessShutdownUtil,
  FutureUtil,
  MonadUtil,
  SimpleExecutionQueue,
}
import com.digitalasset.canton.{LedgerSubmissionId, SequencerCounter}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Listens to changes of the topology stores and notifies the Ledger API server
  *
  * We need to send `PartyAddedToParticipant` messages to Ledger API server for every
  * successful addition with a known participant ID.
  */
class LedgerServerPartyNotifier(
    participantId: ParticipantId,
    eventPublisher: ParticipantEventPublisher,
    store: PartyMetadataStore,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    mustTrackSubmissionIds: Boolean,
    exitOnFatalFailures: Boolean,
    maxItemsInBatch: PositiveInt,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val pendingAllocationData =
    TrieMap[(PartyId, ParticipantId), String255]()
  def expectPartyAllocationForNodes(
      party: PartyId,
      onParticipant: ParticipantId,
      submissionId: String255,
  ): Either[String, Unit] = if (mustTrackSubmissionIds) {
    pendingAllocationData
      .putIfAbsent((party, onParticipant), submissionId)
      .toLeft(())
      .leftMap(_ => s"Allocation for party $party is already inflight")
  } else
    Either.unit

  def expireExpectedPartyAllocationForNodes(
      party: PartyId,
      onParticipant: ParticipantId,
      submissionId: String255,
  ): Unit = {
    val key = (party, onParticipant)
    pendingAllocationData.get(key).foreach { case storedId =>
      if (storedId == submissionId) {
        pendingAllocationData.remove(key).discard
      }
    }
  }

  def resumePending(): Future[Unit] = {
    import TraceContext.Implicits.Empty.*
    store.fetchNotNotified().map { unnotified =>
      if (unnotified.nonEmpty)
        logger.debug(s"Resuming party notification with ${unnotified.size} pending notifications")
      val partiesMetadataO =
        NonEmpty
          .from(
            unnotified
              .collect { case withParticipant @ PartyMetadata(_, Some(_)) => withParticipant }
              .groupBy(_.effectiveTimestamp)
              .flatMap { case (effectiveAt, partiesToNotify) =>
                partiesToNotify
                  .grouped(maxItemsInBatch.unwrap)
                  .map(
                    effectiveAt -> NonEmpty
                      .from(_)
                      .getOrElse(
                        throw new IllegalStateException("grouped sequences cannot be empty")
                      )
                  )
              }
          )
      partiesMetadataO.foreach(scheduleNotifications(_, SequencedTime(clock.now)))
    }
  }

  def attachToTopologyProcessor(): TopologyTransactionProcessingSubscriber =
    new TopologyTransactionProcessingSubscriber {

      override def observed(
          sequencerTimestamp: SequencedTime,
          effectiveTimestamp: EffectiveTime,
          sequencerCounter: SequencerCounter,
          transactions: Seq[GenericSignedTopologyTransaction],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        observeTopologyTransactions(sequencerTimestamp, effectiveTimestamp, transactions)
    }

  def observeTopologyTransactions(
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    MonadUtil.sequentialTraverse_(
      transactions
        .flatMap(extractTopologyProcessorData)
        .grouped(maxItemsInBatch.unwrap)
    )(observedF(sequencedTime, effectiveTime, _))

  private def extractTopologyProcessorData(
      transaction: GenericSignedTopologyTransaction
  ): Seq[(PartyId, Option[ParticipantId], String255)] =
    if (transaction.operation != TopologyChangeOp.Replace || transaction.isProposal) {
      Seq.empty
    } else {
      transaction.mapping match {
        case PartyToParticipant(partyId, _, participants) =>
          participants
            .map { hostingParticipant =>
              // Note/CN-5291: Only remove pending submission-id once update persisted.
              val submissionId = pendingAllocationData
                .getOrElse(
                  (partyId, hostingParticipant.participantId),
                  LengthLimitedString.getUuid.asString255,
                )
              (
                partyId,
                Some(hostingParticipant.participantId),
                submissionId,
              )
            }
        // propagate admin parties
        case DomainTrustCertificate(participantId, _) =>
          Seq(
            (
              participantId.adminParty,
              Some(participantId),
              LengthLimitedString.getUuid.asString255,
            )
          )
        case _ => Seq.empty
      }
    }

  private val sequentialQueue = new SimpleExecutionQueue(
    "LedgerServerPartyNotifier",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private def updateAndNotify(
      parties: Seq[(PartyId, Option[ParticipantId], String255)],
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      storedParties <- store.metadataForParties(parties.map { case (partyId, _, _) => partyId })

      partiesAndStored = parties.zip(storedParties)

      partiesByEffectiveTimeO = NonEmpty
        .from(
          partiesAndStored
            .flatMap {
              case ((partyId, targetParticipantIdO, submissionId), None) =>
                Some(
                  PartyMetadata(partyId, targetParticipantIdO)(
                    effectiveTimestamp.value,
                    submissionId,
                  )
                ): Option[PartyMetadata]
              case ((partyId, targetParticipantIdO, submissionId), Some(stored)) =>
                // Compare the inputs of `updateAndNotify` with the party metadata retrieved from the store
                // Returns `None` if there are no actual updates to record, otherwise `Some` with the actual update
                // For the most part, different submissions (with different submission IDs) will always represent
                // an update even if nothing else has changed.
                // Assumption: `stored.partyId == partyId`
                val update =
                  PartyMetadata(
                    partyId = partyId,
                    participantId = // Don't overwrite the participant ID if it's already set to the local participant
                      if (stored.participantId.contains(participantId)) stored.participantId
                      else targetParticipantIdO.orElse(stored.participantId),
                  )(
                    effectiveTimestamp = effectiveTimestamp.value.max(stored.effectiveTimestamp),
                    submissionId = submissionId,
                  )
                if (stored != update) Some(update)
                else {
                  logger.debug(
                    s"Not applying duplicate party metadata update with submission ID $submissionId"
                  )
                  None
                }
            }
            .groupBy1(_.effectiveTimestamp)
        )

      _ <- partiesByEffectiveTimeO
        .fold(Future.unit)(applyUpdateAndNotify(_, sequencerTimestamp))
    } yield ()

  private def applyUpdateAndNotify(
      partiesByEffectiveTime: NonEmpty[Map[CantonTimestamp, NonEmpty[Seq[PartyMetadata]]]],
      sequencerTimestamp: SequencedTime,
  )(implicit traceContext: TraceContext): Future[Unit] =
    for (
      _ <- store.insertOrUpdatePartyMetadata(
        partiesByEffectiveTime.flatMap { case (_, v) => v }.toSeq
      )
    )
      yield {
        // Clear the expected submissionId only after the party metadata has been stored
        // in case of races (such as https://github.com/DACH-NY/canton-network-node/issues/5291).
        // Further races are prevented by this function being called (indirectly) within the
        // sequential queue.
        partiesByEffectiveTime.foreach { case (_, partyMetadataSeq) =>
          partyMetadataSeq
            .foreach(metadata =>
              metadata.participantId.foreach(participant =>
                pendingAllocationData.remove((metadata.partyId, participant)).discard
              )
            )
        }

        scheduleNotifications(partiesByEffectiveTime, sequencerTimestamp)
      }

  private def scheduleNotifications(
      partiesByEffectiveTime: NonEmpty[Map[CantonTimestamp, NonEmpty[Seq[PartyMetadata]]]],
      sequencerTimestamp: SequencedTime,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    partiesByEffectiveTime.foreach { case (effectiveTimestamp, parties) =>
      logger.debug(
        s"About to schedule notifications for ${parties.size} effective at $effectiveTimestamp"
      )
      // Delays the notification to ensure that the topology change is visible to the ledger server
      // This approach relies on the local `clock` not to drift too much away from the sequencer
      PositiveFiniteDuration
        .create(effectiveTimestamp - sequencerTimestamp.value)
        .map(_.duration)
        .toOption match {
        case Some(timeBeforeScheduling) =>
          logger.debug(s"Scheduling notification in $timeBeforeScheduling")
          clock
            .scheduleAfter(notifyLedgerServer(effectiveTimestamp, parties), timeBeforeScheduling)
            .discard
        case None =>
          logger.debug("Doing notification now")
          notifyLedgerServer(effectiveTimestamp, parties)(clock.now)
      }
    }

  private def sendNotifications(
      partyMetadataSeq: NonEmpty[Seq[PartyMetadata]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updates = partyMetadataSeq.forgetNE.flatMap {
      case metadata @ PartyMetadata(_, Some(hostingParticipant)) =>
        logger.debug(show"Pushing ${metadata.partyId} on $hostingParticipant to ledger server")
        Some(
          Update.PartyAddedToParticipant(
            metadata.partyId.toLf,
            hostingParticipant.toLf,
            ParticipantEventPublisher.now,
            LedgerSubmissionId.fromString(metadata.submissionId.unwrap).toOption,
          )
        )
      case metadata @ PartyMetadata(_, None) =>
        logger.debug(
          s"Skipping party metadata ledger server notification because the participant ID is missing $metadata"
        )
        None
    }
    if (updates.nonEmpty) {
      eventPublisher.publishEventsDelayableByRepairOperation(updates)
    } else {
      FutureUnlessShutdown.unit
    }
  }

  private def notifyLedgerServer(
      effectiveTimestamp: CantonTimestamp,
      partyMetadataSeq: NonEmpty[Seq[PartyMetadata]],
  )(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    val parties = partyMetadataSeq.forgetNE.map(_.partyId).mkString(",")
    FutureUtil.doNotAwait(
      sequentialQueue
        .executeUS(
          for {
            _ <- sendNotifications(partyMetadataSeq)
            _ <- FutureUnlessShutdown.outcomeF(
              store.markNotified(effectiveTimestamp, partyMetadataSeq.map(_.partyId))
            )
          } yield {
            logger.debug(s"Notification for $parties scheduled at $timestamp sent and marked.")
          },
          s"Notifying the ledger server about the metadata update of $parties",
        )
        .unwrap,
      s"Error while sending the metadata update notification for $parties to the ledger server",
    )
  }

  private def observedF(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      parties: Seq[(PartyId, Option[ParticipantId], String255)],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    // note, that if this fails, we have an issue as ledger server will not have
    // received the event. this is generally an issue with everything we send to the
    // index server
    FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
      sequentialQueue.execute(
        updateAndNotify(parties, sequencerTimestamp, effectiveTimestamp),
        s"notify ledger server about parties ${parties.map(_._1).mkString(", ")}",
      ),
      s"Notifying ledger server about transaction failed",
    )

  override protected def onClosed(): Unit = {
    LifeCycle.close(sequentialQueue)(logger)
    super.onClosed()
  }

}
