// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
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
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val pendingAllocationData =
    TrieMap[(PartyId, ParticipantId), (String255, Option[DisplayName])]()
  def expectPartyAllocationForXNodes(
      party: PartyId,
      onParticipant: ParticipantId,
      submissionId: String255,
      displayName: Option[DisplayName],
  ): Either[String, Unit] = if (mustTrackSubmissionIds) {
    pendingAllocationData
      .putIfAbsent((party, onParticipant), (submissionId, displayName))
      .toLeft(())
      .leftMap(_ => s"Allocation for party ${party} is already inflight")
  } else
    Right(())

  def expireExpectedPartyAllocationForXNodes(
      party: PartyId,
      onParticipant: ParticipantId,
      submissionId: String255,
  ): Unit = {
    val key = (party, onParticipant)
    pendingAllocationData.get(key).foreach { case (storedId, _) =>
      if (storedId == submissionId) {
        pendingAllocationData.remove(key).discard
      }
    }
  }

  def resumePending(): Future[Unit] = {
    import TraceContext.Implicits.Empty.*
    store.fetchNotNotified().map { todo =>
      if (todo.nonEmpty)
        logger.debug(s"Resuming party notification with ${todo.size} pending notifications")
      todo.foreach { partyMetadata =>
        val participantIdO = partyMetadata.participantId
        participantIdO.foreach(_ => scheduleNotification(partyMetadata, SequencedTime(clock.now)))
      }
    }
  }

  def attachToTopologyProcessorX(): TopologyTransactionProcessingSubscriberX =
    new TopologyTransactionProcessingSubscriberX {

      override def observed(
          sequencerTimestamp: SequencedTime,
          effectiveTimestamp: EffectiveTime,
          sequencerCounter: SequencerCounter,
          transactions: Seq[GenericSignedTopologyTransactionX],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        observeTopologyTransactions(sequencerTimestamp, effectiveTimestamp, transactions)
      }
    }

  def attachToIdentityManagerX(): TopologyManagerObserver =
    new TopologyManagerObserver {
      override def addedNewTransactions(
          timestamp: CantonTimestamp,
          transactions: Seq[GenericSignedTopologyTransactionX],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        observeTopologyTransactions(
          SequencedTime(timestamp),
          EffectiveTime(timestamp),
          transactions,
        )
    }

  def observeTopologyTransactions(
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    transactions.parTraverse_(
      extractTopologyProcessorXData(_)
        .parTraverse_(observedF(sequencedTime, effectiveTime, _))
    )
  }

  private def extractTopologyProcessorXData(
      transaction: GenericSignedTopologyTransactionX
  ): Seq[(PartyId, ParticipantId, String255, Option[DisplayName])] = {
    if (transaction.operation != TopologyChangeOpX.Replace || transaction.isProposal) {
      Seq.empty
    } else {
      transaction.transaction.mapping match {
        case PartyToParticipantX(partyId, _, _, participants, _) =>
          participants
            .map { hostingParticipant =>
              // Note/CN-5291: Only remove pending submission-id once update persisted.
              val (submissionId, displayName) = pendingAllocationData
                .getOrElse(
                  (partyId, hostingParticipant.participantId),
                  (LengthLimitedString.getUuid.asString255, None),
                )
              (
                partyId,
                hostingParticipant.participantId,
                submissionId,
                displayName,
              )
            }
        // propagate admin parties
        case DomainTrustCertificateX(participantId, _, _, _) =>
          Seq(
            (
              participantId.adminParty,
              participantId,
              LengthLimitedString.getUuid.asString255,
              None,
            )
          )
        case _ => Seq.empty
      }
    }
  }

  private val sequentialQueue = new SimpleExecutionQueue(
    "LedgerServerPartyNotifier",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  def setDisplayName(partyId: PartyId, displayName: DisplayName)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    sequentialQueue
      .execute(
        {
          val currentTime = clock.now
          logger.debug(
            s"Setting display name ${displayName.singleQuoted} for party $partyId as of $currentTime"
          )
          updateAndNotify(
            partyId,
            Some(displayName),
            None,
            SequencedTime(currentTime),
            EffectiveTime(currentTime),
          )
        },
        s"set display name for $partyId",
      )
      .onShutdown(logger.debug("Shutdown in progress, canceling display name update"))

  private def updateAndNotify(
      partyId: PartyId,
      displayName: Option[DisplayName],
      targetParticipantId: Option[ParticipantId],
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      submissionIdRaw: String255 = LengthLimitedString.getUuid.asString255,
  )(implicit traceContext: TraceContext): Future[Unit] = {

    // Compare the inputs of `updateAndNotify` with the party metadata retrieved from the store
    // Returns `None` if there are no actual updates to record, otherwise `Some` with the actual update
    // For the most part, different submissions (with different submission IDs) will always represent
    // an update even if nothing else has changed.
    // Assumption: `current.partyId == partyId`
    def computeUpdateOver(current: PartyMetadata): Option[PartyMetadata] = {
      val update = {
        PartyMetadata(
          partyId = partyId,
          displayName = displayName.orElse(current.displayName),
          participantId = // Don't overwrite the participant ID if it's already set to the expected value
            if (current.participantId.contains(participantId)) current.participantId
            else targetParticipantId.orElse(current.participantId),
        )(
          effectiveTimestamp = effectiveTimestamp.value.max(current.effectiveTimestamp),
          submissionId = submissionIdRaw,
        )
      }
      Option.when(current != update)(update)
    }

    val maybeUpdate: Future[Option[PartyMetadata]] =
      store.metadataForParty(partyId).map {
        case None =>
          Some(
            PartyMetadata(partyId, displayName, targetParticipantId)(
              effectiveTimestamp.value,
              submissionIdRaw,
            )
          )
        case Some(current) =>
          computeUpdateOver(current)
      }

    maybeUpdate.flatMap {
      case Some(update) =>
        applyUpdateAndNotify(update, sequencerTimestamp)
      case None =>
        logger.debug(
          s"Not applying duplicate party metadata update with submission ID $submissionIdRaw"
        )
        Future.unit
    }

  }

  private def applyUpdateAndNotify(
      metadata: PartyMetadata,
      sequencerTimestamp: SequencedTime,
  )(implicit traceContext: TraceContext): Future[Unit] =
    for (_ <- store.insertOrUpdatePartyMetadata(metadata)) yield {
      // Clear the expected submissionId only after the party metadata has been stored
      // in case of races (such as https://github.com/DACH-NY/canton-network-node/issues/5291).
      // Further races are prevented by this function being called (indirectly) within the
      // sequential queue.
      metadata.participantId.foreach(participant =>
        pendingAllocationData.remove((metadata.partyId, participant)).discard
      )

      scheduleNotification(metadata, sequencerTimestamp)
    }

  private def scheduleNotification(
      metadata: PartyMetadata,
      sequencerTimestamp: SequencedTime,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    // Delays the notification to ensure that the topology change is visible to the ledger server
    // This approach relies on the local `clock` not to drift to much away from the sequencer
    PositiveFiniteDuration
      .create(metadata.effectiveTimestamp - sequencerTimestamp.value)
      .map(_.duration)
      .toOption match {
      case Some(timeBeforeScheduling) =>
        lazy val latestMetadata = checkForConcurrentUpdate(metadata)
        clock
          .scheduleAfter(notifyLedgerServer(metadata.partyId, latestMetadata), timeBeforeScheduling)
          .discard
      case None =>
        notifyLedgerServer(metadata.partyId, Future.successful(metadata))(clock.now)
    }
  }

  private def checkForConcurrentUpdate(current: PartyMetadata)(implicit
      traceContext: TraceContext
  ): Future[PartyMetadata] =
    for (metadata <- store.metadataForParty(current.partyId)) yield {
      metadata
        .collect {
          case stored if stored.effectiveTimestamp > current.effectiveTimestamp =>
            // Keep the submission ID as is to ensure the ledger server recognizes this message
            stored.copy()(stored.effectiveTimestamp, current.submissionId, stored.notified)
        }
        .getOrElse(current)
    }

  private def sendNotification(
      metadata: PartyMetadata
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    metadata.participantId match {
      case Some(participantId) =>
        logger.debug(show"Pushing ${metadata.partyId} on $participantId to ledger server")
        eventPublisher.publish(
          LedgerSyncEvent.PartyAddedToParticipant(
            metadata.partyId.toLf,
            metadata.displayName.map(_.unwrap).getOrElse(""),
            participantId.toLf,
            ParticipantEventPublisher.now.toLf,
            LedgerSubmissionId.fromString(metadata.submissionId.unwrap).toOption,
          )
        )
      case None =>
        FutureUnlessShutdown.pure(
          logger.debug(
            s"Skipping party metadata ledger server notification because the participant ID is missing $metadata"
          )
        )
    }

  private def notifyLedgerServer(
      partyId: PartyId,
      fetchMetadata: => Future[PartyMetadata],
  )(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit =
    FutureUtil.doNotAwait(
      sequentialQueue
        .executeUS(
          for {
            metadata <- FutureUnlessShutdown.outcomeF(fetchMetadata)
            _ <- sendNotification(metadata)
            _ <- FutureUnlessShutdown.outcomeF(store.markNotified(metadata))
          } yield {
            logger.debug(s"Notification for $partyId scheduled at $timestamp sent and marked")
          },
          s"Notifying the ledger server about the metadata update of $partyId",
        )
        .unwrap,
      s"Error while sending the metadata update notification for $partyId to the ledger server",
    )

  private def observedF(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      data: (PartyId, ParticipantId, String255, Option[DisplayName]),
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    val (party, participant, submissionId, displayName) = data
    // start the notification in the background
    // note, that if this fails, we have an issue as ledger server will not have
    // received the event. this is generally an issue with everything we send to the
    // index server
    FutureUtil.logOnFailureUnlessShutdown(
      sequentialQueue.execute(
        updateAndNotify(
          party,
          displayName = displayName,
          targetParticipantId = Some(participant),
          sequencerTimestamp,
          effectiveTimestamp,
          submissionId,
        ),
        s"notify ledger server about $party",
      ),
      s"Notifying ledger server about transaction failed",
    )
  }

  override protected def onClosed(): Unit = {
    Lifecycle.close(sequentialQueue)(logger)
    super.onClosed()
  }

}
