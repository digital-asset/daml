// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdownFactory}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.store.{
  ContractStore,
  ParticipantNodeEphemeralState,
  RequestJournalStore,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.util.{TimeOfChange, TimeOfRequest}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.store.SequencedEventStore.ByTimestamp
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{RepairCounter, RequestCounter, SequencerCounter}

import scala.concurrent.ExecutionContext

trait SyncEphemeralStateFactory {
  def createFromPersistent(
      persistentState: SyncPersistentState,
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      contractStore: Eval[ContractStore],
      participantNodeEphemeralState: ParticipantNodeEphemeralState,
      createTimeTracker: () => SynchronizerTimeTracker,
      promiseUSFactory: PromiseUnlessShutdownFactory,
      metrics: ConnectedSynchronizerMetrics,
      sessionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SyncEphemeralState]
}

class SyncEphemeralStateFactoryImpl(
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
)(implicit ec: ExecutionContext)
    extends SyncEphemeralStateFactory
    with NamedLogging {

  override def createFromPersistent(
      persistentState: SyncPersistentState,
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      contractStore: Eval[ContractStore],
      participantNodeEphemeralState: ParticipantNodeEphemeralState,
      createTimeTracker: () => SynchronizerTimeTracker,
      promiseUSFactory: PromiseUnlessShutdownFactory,
      metrics: ConnectedSynchronizerMetrics,
      sessionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SyncEphemeralState] =
    for {
      _ <- ledgerApiIndexer.value.ensureNoProcessingForSynchronizer(
        persistentState.indexedSynchronizer.synchronizerId
      )
      synchronizerIndex <- ledgerApiIndexer.value.ledgerApiStore.value
        .cleanSynchronizerIndex(persistentState.indexedSynchronizer.synchronizerId)
      startingPoints <- SyncEphemeralStateFactory.startingPoints(
        persistentState.requestJournalStore,
        persistentState.sequencedEventStore,
        synchronizerIndex,
      )

      _ <- SyncEphemeralStateFactory.cleanupPersistentState(persistentState, synchronizerIndex)

      recordOrderPublisher = new RecordOrderPublisher(
        persistentState.indexedSynchronizer.synchronizerId,
        startingPoints.processing.nextSequencerCounter,
        startingPoints.processing.currentRecordTime,
        ledgerApiIndexer.value,
        metrics.recordOrderPublisher,
        exitOnFatalFailures = exitOnFatalFailures,
        timeouts,
        loggerFactory,
        futureSupervisor,
        clock,
      )

      // the time tracker, note, must be shutdown in synchronizer as it is using the sequencer client to
      // request time proofs.
      timeTracker = createTimeTracker()

      inFlightSubmissionSynchronizerTracker <-
        participantNodeEphemeralState.inFlightSubmissionTracker
          .inFlightSubmissionSynchronizerTracker(
            synchronizerId = persistentState.indexedSynchronizer.synchronizerId,
            recordOrderPublisher = recordOrderPublisher,
            timeTracker = timeTracker,
            metrics = metrics,
          )
    } yield {
      logger.debug("Created SyncEphemeralState")
      new SyncEphemeralState(
        participantId,
        recordOrderPublisher,
        timeTracker,
        inFlightSubmissionSynchronizerTracker,
        persistentState,
        ledgerApiIndexer.value,
        contractStore.value,
        promiseUSFactory,
        startingPoints,
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

object SyncEphemeralStateFactory {

  /** Returns the starting points for replaying of clean requests and for processing messages.
    * Replaying of clean requests reconstructs the ephemeral state at the point where processing
    * resumes.
    *
    * Processing resumes at the next request after the clean synchronizer Index stored together with
    * the LedgerEnd by the indexer. If no such next request is known, this is immediately after the
    * clean head synchronizer Index's request's timestamp or
    * [[com.digitalasset.canton.participant.protocol.MessageProcessingStartingPoint.default]] if
    * there is no clean synchronizer Index.
    *
    * The starting point for replaying of clean requests starts with the first request (by request
    * counter) whose commit time is after the starting point for processing messages. If there is no
    * such request, the starting point for replaying is the same as the one for processing messages.
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
      synchronizerIndexO: Option[SynchronizerIndex],
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[ProcessingStartingPoints] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    for {
      requestCounterO <- synchronizerIndexO
        .flatTraverse(si =>
          requestJournalStore.lastRequestTimeWithRequestTimestampBeforeOrAt(si.recordTime)
        )
      sequencerCounterO <- synchronizerIndexO
        .flatMap(_.sequencerIndex)
        .traverse(sequencerIndex =>
          sequencedEventStore
            .find(ByTimestamp(sequencerIndex.sequencerTimestamp))
            .value
            .map(
              _.getOrElse(
                ErrorUtil.invalidState(
                  s"SequencerIndex with timestamp ${sequencerIndex.sequencerTimestamp} is not found in sequenced event store"
                )
              ).counter
            )
        )
      messageProcessingStartingPoint = MessageProcessingStartingPoint(
        nextRequestCounter = requestCounterO.map(_.rc + 1).getOrElse(RequestCounter.Genesis),
        nextSequencerCounter = sequencerCounterO
          .map(_ + 1)
          .getOrElse(SequencerCounter.Genesis),
        lastSequencerTimestamp = lastSequencerTimestamp(synchronizerIndexO),
        currentRecordTime = currentRecordTime(synchronizerIndexO),
        nextRepairCounter = nextRepairCounter(synchronizerIndexO),
      )
      replayOpt <- requestJournalStore
        .firstRequestWithCommitTimeAfter(
          // We need to follow the repair requests which might come between sequencer timestamps hence we
          // use here messageProcessingStartingPoint.currentRecordTime.
          // MessageProcessingStartingPoint.currentRecordTime is always before the next sequencer timestamp.
          messageProcessingStartingPoint.currentRecordTime
        )
      cleanReplayStartingPoint <- replayOpt
        .filter(_.rc < messageProcessingStartingPoint.nextRequestCounter)
        .map(replay =>
          // This request cannot be a repair request on an empty synchronizer because a repair request on the empty synchronizer
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
          FutureUnlessShutdown.pure(
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
      loggingContext.info(show"Computed starting points: $startingPoints")
      startingPoints
    }
  }

  private def lastSequencerTimestampO(
      synchronizerIndexO: Option[SynchronizerIndex]
  ): Option[CantonTimestamp] =
    synchronizerIndexO
      .flatMap(_.sequencerIndex)
      .map(_.sequencerTimestamp)

  def lastSequencerTimestamp(synchronizerIndexO: Option[SynchronizerIndex]): CantonTimestamp =
    lastSequencerTimestampO(synchronizerIndexO)
      .getOrElse(CantonTimestamp.MinValue)

  def currentRecordTime(synchronizerIndexO: Option[SynchronizerIndex]): CantonTimestamp =
    synchronizerIndexO
      .map(_.recordTime)
      .getOrElse(CantonTimestamp.MinValue)

  def nextRepairCounter(synchronizerIndexO: Option[SynchronizerIndex]): RepairCounter =
    synchronizerIndexO
      .flatMap(si =>
        si.repairIndex.collect {
          // Filter out repairs that are not at the record time, as the repair counter
          // is relative to its associated timestamp and restarts are genesis on new timestamps.
          case ri if ri.timestamp == si.recordTime => ri.counter + 1
        }
      )
      .getOrElse(RepairCounter.Genesis)

  /** Returns an upper bound for the timestamps up to which pruning may remove data from the stores
    * (inclusive) so that crash recovery will still work.
    */
  def crashRecoveryPruningBoundInclusive(
      requestJournalStore: RequestJournalStore,
      cleanSynchronizerIndexO: Option[SynchronizerIndex],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[CantonTimestamp] =
    // Crash recovery cleans up the stores before replay starts,
    // however we may have used some of the deleted information to determine the starting points for the replay.
    // So if a crash occurs during crash recovery, we may start again and come up with an earlier processing starting point.
    // We want to make sure that crash recovery access only data whose timestamps comes after what pruning is allowed to delete.
    // This method returns a timestamp that is before the data that crash recovery accesses after any number of iterating
    // the computation of starting points and crash recovery clean-ups.
    //
    // The earliest possible starting point is the earlier of the following:
    // * The first request whose commit time is after the clean synchronizer index timestamp
    // * The clean sequencer counter prehead timestamp
    for {
      cleanTimeOfRequest <- cleanSynchronizerIndexO
        .fold[FutureUnlessShutdown[Option[TimeOfRequest]]](FutureUnlessShutdown.pure(None))(
          synchronizerIndex =>
            requestJournalStore
              .lastRequestTimeWithRequestTimestampBeforeOrAt(synchronizerIndex.recordTime)
        )
      requestReplayTs <- cleanTimeOfRequest match {
        case None =>
          // No request is known to be clean, nothing can be pruned
          FutureUnlessShutdown.pure(CantonTimestamp.MinValue)
        case Some(timeOfRequest) =>
          requestJournalStore.firstRequestWithCommitTimeAfter(timeOfRequest.timestamp).map { res =>
            val ts = res.fold(timeOfRequest.timestamp)(_.requestTimestamp)
            // If the only processed requests so far are repair requests, it can happen that `ts == CantonTimestamp.MinValue`.
            // Taking the predecessor throws an exception.
            if (ts == CantonTimestamp.MinValue) ts else ts.immediatePredecessor
          }
      }
      // TODO(i21246): Note for unifying crashRecoveryPruningBoundInclusive and startingPoints: This minimum building is not needed anymore, as the request timestamp is also smaller than the sequencer timestamp.
      cleanSequencerIndexTs = cleanSynchronizerIndexO
        .flatMap(_.sequencerIndex)
        .fold(CantonTimestamp.MinValue)(_.sequencerTimestamp.immediatePredecessor)
    } yield requestReplayTs.min(cleanSequencerIndexTs)

  def cleanupPersistentState(
      persistentState: SyncPersistentState,
      synchronizerIndexO: Option[SynchronizerIndex],
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[Unit] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val logger = loggingContext.logger
    val nextSequencerTimestamp = lastSequencerTimestampO(synchronizerIndexO)
      .map(_.immediateSuccessor)
      .getOrElse(CantonTimestamp.MinValue)
    val nextTimeOfChange = TimeOfChange(
      ts = currentRecordTime(synchronizerIndexO),
      counterO = Some(nextRepairCounter(synchronizerIndexO)),
    )
    logger.debug("Deleting dirty requests")
    for {
      _ <- persistentState.requestJournalStore.deleteSinceRequestTimestamp(
        // The nextSequencerTimestamp is a lower bound (smaller or equal than to) for the requestTimestamp for the nextSequencerCounter
        nextSequencerTimestamp
      )
      _ = logger.debug("Deleting contract activeness changes")
      _ <- persistentState.activeContractStore.deleteSince(nextTimeOfChange)
      _ = logger.debug("Deleting reassignment completions")
      _ <- persistentState.reassignmentStore.deleteCompletionsSince(nextSequencerTimestamp)
      _ = logger.debug("Deleting registered fresh requests")
      _ <- persistentState.submissionTrackerStore.deleteSince(nextSequencerTimestamp)
    } yield ()
  }
}
