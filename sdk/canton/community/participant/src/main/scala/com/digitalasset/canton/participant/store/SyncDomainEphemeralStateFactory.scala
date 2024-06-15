// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionKeyCacheConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.RequestOffset
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.sequencing.PossiblyIgnoredSerializedEvent
import com.digitalasset.canton.store.CursorPrehead.{
  RequestCounterCursorPrehead,
  SequencerCounterCursorPrehead,
}
import com.digitalasset.canton.store.SequencedEventStore.{ByTimestamp, LatestUpto}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{RequestCounter, SequencerCounter, checked}

import scala.concurrent.{ExecutionContext, Future}

trait SyncDomainEphemeralStateFactory {
  def createFromPersistent(
      persistentState: SyncDomainPersistentState,
      multiDomainEventLog: Eval[MultiDomainEventLog],
      inFlightSubmissionTracker: InFlightSubmissionTracker,
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
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainEphemeralStateFactory
    with NamedLogging {

  override def createFromPersistent(
      persistentState: SyncDomainPersistentState,
      multiDomainEventLog: Eval[MultiDomainEventLog],
      inFlightSubmissionTracker: InFlightSubmissionTracker,
      createTimeTracker: () => DomainTimeTracker,
      metrics: SyncDomainMetrics,
      sessionKeyCacheConfig: SessionKeyCacheConfig,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[SyncDomainEphemeralState] = {
    for {
      startingPoints <- SyncDomainEphemeralStateFactory.startingPoints(
        persistentState.domainId,
        persistentState.requestJournalStore,
        persistentState.sequencerCounterTrackerStore,
        persistentState.sequencedEventStore,
        multiDomainEventLog.value,
      )
      _ <- SyncDomainEphemeralStateFactory.cleanupPersistentState(persistentState, startingPoints)
    } yield {
      logger.debug("Created SyncDomainEphemeralState")
      new SyncDomainEphemeralState(
        participantId,
        persistentState,
        multiDomainEventLog,
        inFlightSubmissionTracker,
        startingPoints,
        createTimeTracker,
        metrics,
        sessionKeyCacheConfig,
        timeouts,
        persistentState.loggerFactory,
        futureSupervisor,
      )
    }
  }
}

object SyncDomainEphemeralStateFactory {

  /** Returns the starting points for replaying of clean requests and for processing messages.
    * Replaying of clean requests reconstructs the ephemeral state at the point
    * where processing resumes.
    *
    * Processing resumes at the next request after the clean head of the
    * [[com.digitalasset.canton.participant.protocol.RequestJournal]].
    * If no such next request is known, this is immediately after the clean head request's timestamp
    * or [[com.digitalasset.canton.participant.protocol.MessageProcessingStartingPoint.default]] if there is no clean head request.
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
      domainId: IndexedDomain,
      requestJournalStore: RequestJournalStore,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      multiDomainEventLog: MultiDomainEventLog,
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[ProcessingStartingPoints] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val logger = loggingContext.logger

    def noEventForRequestTimestamp(rc: RequestCounter)(err: SequencedEventNotFoundError): Nothing =
      ErrorUtil.internalError(
        new IllegalStateException(s"No sequenced event found for request $rc: ${err.criterion}.")
      )

    def isRepairOnEmptyDomain(requestData: RequestJournal.RequestData): Boolean = {
      val RequestJournal.RequestData(
        _rcProcess,
        _state,
        requestTimestamp,
        _commitTime,
        repairContext,
      ) = requestData
      repairContext.nonEmpty && requestTimestamp == RepairService.RepairTimestampOnEmptyDomain
    }

    def processingStartingPointAndRewoundSequencerCounterPrehead(
        cleanSequencerCounterPreheadO: Option[SequencerCounterCursorPrehead],
        cleanRequestPreheadO: Option[RequestCounterCursorPrehead],
    ): Future[
      (MessageProcessingStartingPoint, Option[SequencerCounterCursorPrehead])
    ] = {
      val firstDirtyRc = cleanRequestPreheadO.fold(RequestCounter.Genesis)(_.counter + 1)
      val cleanRequestPreheadLocalOffsetO = cleanRequestPreheadO.map { case CursorPrehead(rc, ts) =>
        RequestOffset(ts, rc)
      }

      // Cap the rewound sequencer counter prehead by the clean sequencer counter prehead
      // so that we do not skip dirty sequencer counters.
      def capRewinding(
          rewoundSequencerCounterPrehead: Option[SequencerCounterCursorPrehead]
      ): Option[SequencerCounterCursorPrehead] =
        cleanSequencerCounterPreheadO.flatMap { prehead =>
          if (rewoundSequencerCounterPrehead.forall(_.timestamp < prehead.timestamp))
            rewoundSequencerCounterPrehead
          else cleanSequencerCounterPreheadO
        }

      val firstReprocessedRequestF = requestJournalStore.query(firstDirtyRc).value
      firstReprocessedRequestF.flatMap {
        case None =>
          loggingContext.logger.debug(
            show"No request after the clean request prehead found; using the clean sequencer counter prehead $cleanSequencerCounterPreheadO."
          )
          // If we don't know of the next request after the processing starting point,
          // we conservatively assume that the next request could be assigned the first timestamp
          // after the clean sequencer counter prehead and after the clean request prehead.
          // This is safe because we register the request with the request journal before a sequencer counter becomes clean.
          // We need to take the maximum of the two because the clean sequencer counter prehead
          // may lag behind the clean request prehead.
          def startAtCleanSequencerCounterPrehead: Future[
            (MessageProcessingStartingPoint, Option[SequencerCounterCursorPrehead])
          ] =
            cleanSequencerCounterPreheadO match {
              case None =>
                Future.successful(MessageProcessingStartingPoint.default -> None)
              case Some(CursorPrehead(preheadSc, preheadScTs)) =>
                val processingStartingPoint =
                  MessageProcessingStartingPoint(
                    cleanRequestPreheadLocalOffsetO,
                    firstDirtyRc,
                    preheadSc + 1L,
                    preheadScTs,
                  )
                Future.successful(processingStartingPoint -> cleanSequencerCounterPreheadO)
            }

          cleanRequestPreheadLocalOffsetO match {
            case None => startAtCleanSequencerCounterPrehead
            case Some(lastCleanOffset @ RequestOffset(requestTimestampPrehead, rcPrehead)) =>
              if (cleanSequencerCounterPreheadO.exists(_.timestamp >= requestTimestampPrehead)) {
                startAtCleanSequencerCounterPrehead
              } else {
                def noEventForRcPrehead(err: SequencedEventNotFoundError): Future[
                  (
                      MessageProcessingStartingPoint,
                      Option[SequencerCounterCursorPrehead],
                  )
                ] = {
                  // No event found for clean request prehead
                  // Maybe the clean request prehead is a repair request on an empty domain
                  val requestDataForRcPreheadF = requestJournalStore
                    .query(rcPrehead)
                    .getOrElse(
                      ErrorUtil.internalError(
                        new IllegalStateException(s"No request found for clean prehead $rcPrehead")
                      )
                    )
                  requestDataForRcPreheadF.map { requestData =>
                    if (isRepairOnEmptyDomain(requestData))
                      MessageProcessingStartingPoint(
                        cleanRequestPrehead = Some(lastCleanOffset),
                        nextRequestCounter = rcPrehead + 1L,
                        nextSequencerCounter = SequencerCounter.Genesis,
                        prenextTimestamp = CantonTimestamp.MinValue,
                      ) -> None
                    else
                      ErrorUtil.internalError(
                        new IllegalStateException(
                          s"No sequenced event found for request $rcPrehead: ${err.criterion}"
                        )
                      )
                  }
                }

                def withEventForRcPrehead(tracedEvent: PossiblyIgnoredSerializedEvent): Future[
                  (
                      MessageProcessingStartingPoint,
                      Option[SequencerCounterCursorPrehead],
                  )
                ] = {
                  val sequencerCounter = tracedEvent.counter
                  val processingStartingPoint =
                    MessageProcessingStartingPoint(
                      Some(lastCleanOffset),
                      rcPrehead + 1L,
                      sequencerCounter + 1L,
                      requestTimestampPrehead,
                    )
                  Future.successful(processingStartingPoint -> cleanSequencerCounterPreheadO)
                }

                sequencedEventStore
                  .find(ByTimestamp(requestTimestampPrehead))
                  .foldF(noEventForRcPrehead, withEventForRcPrehead)
              }
          }

        case Some(
              processData @ RequestJournal
                .RequestData(rcProcess, _state, requestTimestampProcess, _commitTime, repairContext)
            ) =>
          if (isRepairOnEmptyDomain(processData)) {
            logger.debug(
              show"First inflight validation request is repair request $rcProcess on an empty domain."
            )
            Future.successful(
              MessageProcessingStartingPoint(
                cleanRequestPreheadLocalOffsetO,
                rcProcess,
                SequencerCounter.Genesis,
                RepairService.RepairTimestampOnEmptyDomain,
              ) -> cleanSequencerCounterPreheadO
            )
          } else {
            logger.debug(
              show"First inflight validation request $rcProcess at $requestTimestampProcess"
            )

            for {
              startingEvent <- sequencedEventStore
                .find(ByTimestamp(requestTimestampProcess))
                .valueOr(noEventForRequestTimestamp(rcProcess))
              _ = logger.debug(
                show"Found starting sequenced event ${startingEvent.counter} at ${startingEvent.timestamp}"
              )
              startingPointAndRewoundSequencerCounterPrehead <-
                if (repairContext.isEmpty) {
                  // This is not a repair; so the sequenced event will become dirty when we clean up the stores.
                  // Rewind the clean sequencer counter prehead accordingly and the prenext timestamp
                  val preStartingEventF =
                    sequencedEventStore
                      .find(LatestUpto(requestTimestampProcess.immediatePredecessor))
                      .toOption
                      .value
                  preStartingEventF.map {
                    case None =>
                      MessageProcessingStartingPoint(
                        cleanRequestPreheadLocalOffsetO,
                        rcProcess,
                        SequencerCounter.Genesis,
                        CantonTimestamp.MinValue,
                      ) -> None
                    case Some(preStartingEvent) =>
                      val startingPoint = MessageProcessingStartingPoint(
                        cleanRequestPreheadLocalOffsetO,
                        rcProcess,
                        startingEvent.counter,
                        preStartingEvent.timestamp,
                      )
                      val rewoundSequencerCounterPrehead =
                        CursorPrehead(preStartingEvent.counter, preStartingEvent.timestamp)
                      startingPoint -> Some(rewoundSequencerCounterPrehead)
                  }
                } else {
                  // This is a repair, so the sequencer counter will remain clean
                  // as there is no inflight validation request since the clean request prehead.
                  val startingPoint =
                    MessageProcessingStartingPoint(
                      cleanRequestPreheadLocalOffsetO,
                      rcProcess,
                      startingEvent.counter + 1L,
                      startingEvent.timestamp,
                    )
                  Future.successful(startingPoint -> cleanSequencerCounterPreheadO)
                }
            } yield {
              val (startingPoint, rewoundSequencerCounterPrehead) =
                startingPointAndRewoundSequencerCounterPrehead
              startingPoint -> capRewinding(rewoundSequencerCounterPrehead)
            }
          }
      }
    }

    logger.debug(s"Computing starting points for $domainId")
    for {
      lastPublishedRequestOffsetO <- multiDomainEventLog.lastRequestOffset(
        DomainEventLogId(domainId)
      )
      cleanSequencerCounterPrehead <- sequencerCounterTrackerStore.preheadSequencerCounter
      _ = logger.debug(show"Clean sequencer counter prehead is $cleanSequencerCounterPrehead")
      preheadClean <- requestJournalStore.preheadClean
      startingPoints <- preheadClean match {
        case None =>
          logger.debug("No clean request prehead found")
          // There is nothing clean to replay
          for {
            x <- processingStartingPointAndRewoundSequencerCounterPrehead(
              cleanSequencerCounterPrehead,
              None,
            )
          } yield {
            val (processingStartingPoint, rewoundCleanSequencerCounterPrehead) = x
            checked(
              ProcessingStartingPoints.tryCreate(
                cleanReplay = processingStartingPoint.toMessageCleanReplayStartingPoint,
                processing = processingStartingPoint,
                lastPublishedRequestOffset = lastPublishedRequestOffsetO,
                rewoundSequencerCounterPrehead = rewoundCleanSequencerCounterPrehead,
              )
            )
          }

        case Some(cleanRequestPrehead) =>
          logger.debug(show"Found clean request prehead at $cleanRequestPrehead")
          for {
            x <- processingStartingPointAndRewoundSequencerCounterPrehead(
              cleanSequencerCounterPreheadO = cleanSequencerCounterPrehead,
              cleanRequestPreheadO = Some(cleanRequestPrehead),
            )
            (processingStartingPoint, rewoundSequencerCounterPrehead) = x
            firstReplayedRequest <- requestJournalStore.firstRequestWithCommitTimeAfter(
              processingStartingPoint.prenextTimestamp
            )
            _ = logger.debug(s"First replayed request ${firstReplayedRequest
                .map(data => s"${data.rc} at ${data.requestTimestamp} committed at ${data.commitTime}")}")
            replayStartingPoint <- firstReplayedRequest match {
              case Some(
                    RequestJournal.RequestData(
                      rcReplay,
                      _state,
                      requestTimestampReplay,
                      _commitTime,
                      _repairContext,
                    )
                  ) if rcReplay <= cleanRequestPrehead.counter =>
                // This request cannot be a repair request on an empty domain because a repair request on the empty domain
                // commits at CantonTimestamp.MinValue, i.e., its commit time cannot be after the prenext timestamp.
                sequencedEventStore
                  .find(ByTimestamp(requestTimestampReplay))
                  .fold(
                    noEventForRequestTimestamp(rcReplay),
                    event => {
                      logger.debug(s"Found sequenced event ${event.counter} at ${event.timestamp}")
                      MessageCleanReplayStartingPoint(
                        nextRequestCounter = rcReplay,
                        nextSequencerCounter = event.counter,
                        prenextTimestamp = requestTimestampReplay.immediatePredecessor,
                      )
                    },
                  )
              case _ =>
                // No need to replay clean requests
                // because no requests to be reprocessed were in-flight at the processing starting point.
                Future.successful(processingStartingPoint.toMessageCleanReplayStartingPoint)
            }
          } yield checked(
            ProcessingStartingPoints
              .tryCreate(
                replayStartingPoint,
                processingStartingPoint,
                lastPublishedRequestOffsetO,
                rewoundSequencerCounterPrehead,
              )
          )
      }
    } yield {
      logger.info(show"Computed starting points: $startingPoints")
      startingPoints
    }
  }

  /** Returns an upper bound for the timestamps up to which pruning may remove data from the stores (inclusive)
    * so that crash recovery will still work.
    */
  def crashRecoveryPruningBoundInclusive(
      requestJournalStore: RequestJournalStore,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[CantonTimestamp] = {
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
      requestReplayTs <- requestJournalStore.preheadClean.flatMap {
        case None =>
          // No request is known to be clean, nothing can be pruned
          Future.successful(CantonTimestamp.MinValue)
        case Some(cursorHead) =>
          requestJournalStore.firstRequestWithCommitTimeAfter(cursorHead.timestamp).map { res =>
            val ts = res.fold(cursorHead.timestamp)(_.requestTimestamp)
            /*
            If the only processed requests so far are repair requests, it can happen that `ts == CantonTimestamp.MinValue`.
            Taking the predecessor throws an exception.
             */
            if (ts == CantonTimestamp.MinValue) ts else ts.immediatePredecessor
          }
      }
      preheadSequencerCounterTs <- sequencerCounterTrackerStore.preheadSequencerCounter.map {
        _.fold(CantonTimestamp.MinValue)(_.timestamp.immediatePredecessor)
      }
    } yield requestReplayTs.min(preheadSequencerCounterTs)
  }

  def cleanupPersistentState(
      persistentState: SyncDomainPersistentState,
      startingPoints: ProcessingStartingPoints,
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): Future[Unit] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val logger = loggingContext.logger
    for {
      // We're about to clean the inflight validation requests from the stores.
      // Some of the corresponding events may already have become clean.
      // So we rewind the clean sequencer counter prehead first
      _ <- persistentState.sequencerCounterTrackerStore.rewindPreheadSequencerCounter(
        startingPoints.rewoundSequencerCounterPrehead
      )
      // Delete the unpublished events after the clean request if possible
      // If there's a dirty repair request, this will delete its unpublished event from the event log,
      //
      // Some tests overwrite the clean request prehead.
      // We therefore cannot delete all events in the SingleDimensionEventLog after the processingStartingPoint
      // because some of them may already have been published to the MultiDomainEventLog
      processingStartingPoint = startingPoints.processing
      unpublishedOffsetAfterCleanPreheadO =
        if (startingPoints.processingAfterPublished) {
          processingStartingPoint.cleanRequestPrehead
        } else {
          logger.warn(
            s"The clean request prehead ${processingStartingPoint.cleanRequestPrehead} precedes the last published event at ${startingPoints.lastPublishedRequestOffset}. Has the clean request prehead been manipulated?"
          )
          startingPoints.lastPublishedRequestOffset
        }
      _ <- unpublishedOffsetAfterCleanPreheadO.fold(Future.unit) {
        unpublishedOffsetAfterCleanPrehead =>
          logger.debug(
            s"Deleting unpublished events after $unpublishedOffsetAfterCleanPrehead"
          )
          persistentState.eventLog.deleteAfter(unpublishedOffsetAfterCleanPrehead)
      }
      _ = logger.debug("Deleting inflight validation requests")
      _ <- persistentState.requestJournalStore.deleteSince(
        processingStartingPoint.nextRequestCounter
      )
      _ = logger.debug("Deleting contract activeness changes")
      _ <- persistentState.activeContractStore.deleteSince(
        processingStartingPoint.nextRequestCounter
      )
      _ = logger.debug("Deleting transfer completions")
      _ <- persistentState.transferStore.deleteCompletionsSince(
        processingStartingPoint.nextRequestCounter
      )
      _ = logger.debug("Deleting registered fresh requests")
      _ <- persistentState.submissionTrackerStore.deleteSince(
        processingStartingPoint.prenextTimestamp.immediateSuccessor
      )
    } yield ()
  }
}
