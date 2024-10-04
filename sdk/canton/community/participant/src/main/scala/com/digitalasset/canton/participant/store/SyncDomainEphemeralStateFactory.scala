// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionKeyCacheConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.DomainIndex
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.store.SequencedEventStore.ByTimestamp
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

import scala.concurrent.{ExecutionContext, Future}

trait SyncDomainEphemeralStateFactory {
  def createFromPersistent(
      persistentState: SyncDomainPersistentState,
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      participantNodeEphemeralState: ParticipantNodeEphemeralState,
      createTimeTracker: () => DomainTimeTracker,
      metrics: SyncDomainMetrics,
      sessionKeyCacheConfig: SessionKeyCacheConfig,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[SyncDomainEphemeralState]
}

class SyncDomainEphemeralStateFactoryImpl(
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
)(implicit ec: ExecutionContext)
    extends SyncDomainEphemeralStateFactory
    with NamedLogging {

  override def createFromPersistent(
      persistentState: SyncDomainPersistentState,
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      participantNodeEphemeralState: ParticipantNodeEphemeralState,
      createTimeTracker: () => DomainTimeTracker,
      metrics: SyncDomainMetrics,
      sessionKeyCacheConfig: SessionKeyCacheConfig,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[SyncDomainEphemeralState] =
    for {
      _ <- ledgerApiIndexer.value.ensureNoProcessingForDomain(
        persistentState.indexedDomain.domainId
      )
      startingPoints <- SyncDomainEphemeralStateFactory.startingPoints(
        persistentState.requestJournalStore,
        persistentState.sequencedEventStore,
        tc =>
          ledgerApiIndexer.value.ledgerApiStore.value
            .domainIndex(persistentState.indexedDomain.domainId)(tc),
      )
      _ <- SyncDomainEphemeralStateFactory.cleanupPersistentState(persistentState, startingPoints)
    } yield {
      logger.debug("Created SyncDomainEphemeralState")
      new SyncDomainEphemeralState(
        participantId,
        participantNodeEphemeralState,
        persistentState,
        ledgerApiIndexer.value,
        startingPoints,
        createTimeTracker,
        metrics,
        exitOnFatalFailures = exitOnFatalFailures,
        sessionKeyCacheConfig,
        timeouts,
        persistentState.loggerFactory,
        futureSupervisor,
        clock,
      )
    }
}

object SyncDomainEphemeralStateFactory {

  /** Returns the starting points for replaying of clean requests and for processing messages.
    * Replaying of clean requests reconstructs the ephemeral state at the point
    * where processing resumes.
    *
    * Processing resumes at the next request after the clean Domain Index stored together with the LedgerEnd by the indexer.
    * If no such next request is known, this is immediately after the clean head Domain Index's request's timestamp
    * or [[com.digitalasset.canton.participant.protocol.MessageProcessingStartingPoint.default]] if there is no clean Domain Index.
    *
    * The starting point for replaying of clean requests starts with the first request (by request counter)
    * whose commit time is after the starting point for processing messages. If there is no such request,
    * the starting point for replaying is the same as the one for processing messages.
    */
  /* Invariants underlying this method:
   *
   * Every request in the request journal has a timestamp for which there exists a corresponding sequenced event
   * unless the timestamp is CantonTimestamp.MinValue.
   * For non-repair requests, this is the sequenced event that contains this request.
   * For repair requests, it can be any sequenced event;
   * if this event happens to be a request, then the repair request has a higher request counter than
   * the request in the sequenced event.
   *
   * We're processing all other types of events (ACS commitments, topology transactions)
   * before we assign the next request counter.
   */
  def startingPoints(
      requestJournalStore: RequestJournalStore,
      sequencedEventStore: SequencedEventStore,
      domainIndexF: TraceContext => Future[DomainIndex],
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[ProcessingStartingPoints] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext

    for {
      domainIndex <- domainIndexF(traceContext)
      messageProcessingStartingPoint = MessageProcessingStartingPoint(
        nextRequestCounter = domainIndex.requestIndex
          .map(_.counter + 1)
          .getOrElse(RequestCounter.Genesis),
        nextSequencerCounter = domainIndex.sequencerIndex
          .map(_.counter + 1)
          .getOrElse(SequencerCounter.Genesis),
        prenextTimestamp = domainIndex.sequencerIndex
          .map(_.timestamp)
          .getOrElse(CantonTimestamp.MinValue),
      )
      replayOpt <- requestJournalStore
        .firstRequestWithCommitTimeAfter(
          messageProcessingStartingPoint.prenextTimestamp
        )
      cleanReplayStartingPoint <- replayOpt
        .filter(_.rc < messageProcessingStartingPoint.nextRequestCounter)
        .map(replay =>
          // This request cannot be a repair request on an empty domain because a repair request on the empty domain
          // commits at CantonTimestamp.MinValue, i.e., its commit time cannot be after the prenext timestamp.
          sequencedEventStore
            .find(ByTimestamp(replay.requestTimestamp))
            .fold(
              err =>
                ErrorUtil.internalError(
                  new IllegalStateException(
                    s"No sequenced event found for request ${replay.rc}: ${err.criterion}."
                  )
                ),
              event =>
                MessageCleanReplayStartingPoint(
                  nextRequestCounter = replay.rc,
                  nextSequencerCounter = event.counter,
                  prenextTimestamp = replay.requestTimestamp.immediatePredecessor,
                ),
            )
        )
        .getOrElse(
          Future.successful(
            // No need to replay clean requests
            // because no requests to be reprocessed were in flight at the processing starting point.
            messageProcessingStartingPoint.toMessageCleanReplayStartingPoint
          )
        )
      startingPoints = ProcessingStartingPoints.tryCreate(
        cleanReplayStartingPoint,
        messageProcessingStartingPoint,
      )
    } yield {
      loggingContext.logger.info(show"Computed starting points: $startingPoints")
      startingPoints
    }
  }

  /** Returns an upper bound for the timestamps up to which pruning may remove data from the stores (inclusive)
    * so that crash recovery will still work.
    */
  def crashRecoveryPruningBoundInclusive(
      requestJournalStore: RequestJournalStore,
      domainIndex: DomainIndex,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[CantonTimestamp] =
    // Crash recovery cleans up the stores before replay starts,
    // however we may have used some of the deleted information to determine the starting points for the replay.
    // So if a crash occurs during crash recovery, we may start again and come up with an earlier processing starting point.
    // We want to make sure that crash recovery access only data whose timestamps comes after what pruning is allowed to delete.
    // This method returns a timestamp that is before the data that crash recovery accesses after any number of iterating
    // the computation of starting points and crash recovery clean-ups.
    //
    // The earliest possible starting point is the earlier of the following:
    // * The first request whose commit time is after the clean request prehead timestamp
    // * The clean sequencer counter prehead timestamp
    for {
      requestReplayTs <- domainIndex.requestIndex match {
        case None =>
          // No request is known to be clean, nothing can be pruned
          Future.successful(CantonTimestamp.MinValue)
        case Some(requestIndex) =>
          requestJournalStore.firstRequestWithCommitTimeAfter(requestIndex.timestamp).map { res =>
            val ts = res.fold(requestIndex.timestamp)(_.requestTimestamp)
            // If the only processed requests so far are repair requests, it can happen that `ts == CantonTimestamp.MinValue`.
            // Taking the predecessor throws an exception.
            if (ts == CantonTimestamp.MinValue) ts else ts.immediatePredecessor
          }
      }
      // TODO(i18695): Note for unifying crashRecoveryPruningBoundInclusive and startingPoints: This minimum building is not needed anymore, as the request timestamp is also smaller than the sequencer timestamp.
      preheadSequencerCounterTs = domainIndex.sequencerIndex
        .fold(CantonTimestamp.MinValue)(_.timestamp.immediatePredecessor)
    } yield requestReplayTs.min(preheadSequencerCounterTs)

  def cleanupPersistentState(
      persistentState: SyncDomainPersistentState,
      startingPoints: ProcessingStartingPoints,
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): Future[Unit] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val logger = loggingContext.logger
    val processingStartingPoint = startingPoints.processing
    logger.debug("Deleting dirty requests")
    for {
      _ <- persistentState.requestJournalStore.deleteSince(
        processingStartingPoint.nextRequestCounter
      )
      _ = logger.debug("Deleting contract activeness changes")
      _ <- persistentState.activeContractStore.deleteSince(
        processingStartingPoint.nextRequestCounter
      )
      _ = logger.debug("Deleting reassignment completions")
      _ <- persistentState.reassignmentStore.deleteCompletionsSince(
        processingStartingPoint.nextRequestCounter
      )
      _ = logger.debug("Deleting registered fresh requests")
      _ <- persistentState.submissionTrackerStore.deleteSince(
        processingStartingPoint.prenextTimestamp.immediateSuccessor
      )
    } yield ()
  }
}
