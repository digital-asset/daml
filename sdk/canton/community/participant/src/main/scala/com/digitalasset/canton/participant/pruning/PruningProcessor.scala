// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import cats.{Eval, Monad}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.Pruning
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.metrics.PruningMetrics
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.CommitmentsPruningBound
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  InFlightSubmissionStore,
  ParticipantNodePersistentState,
  RequestJournalStore,
  SyncEphemeralStateFactory,
  SyncPersistentState,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.pruning.ConfigForNoWaitCounterParticipants
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{
  EitherTUtil,
  ErrorUtil,
  FutureUnlessShutdownUtil,
  SimpleExecutionQueue,
}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

/** The pruning processor coordinates the pruning of all participant node stores
  *
  * @param participantNodePersistentState
  *   the persistent state of the participant node that is not specific to a synchronizer
  * @param syncPersistentStateManager
  *   manager to provide a participant's state for a synchronizer used for pruning
  * @param maxPruningBatchSize
  *   size to which to break up pruning batches to limit (memory) resource consumption
  * @param metrics
  *   pruning metrics
  * @param exitOnFatalFailures
  *   whether to crash on failures
  * @param synchronizerConnectionStatus
  *   helper to determine whether the synchronizer is active or in another state
  */
class PruningProcessor(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    syncPersistentStateManager: SyncPersistentStateManager,
    sortedReconciliationIntervalsProviderFactory: SortedReconciliationIntervalsProviderFactory,
    maxPruningBatchSize: PositiveInt,
    metrics: PruningMetrics,
    exitOnFatalFailures: Boolean,
    synchronizerConnectionStatus: SynchronizerId => Option[
      SynchronizerConnectionConfigStore.Status
    ],
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {
  import PruningProcessor.*

  private val executionQueue = new SimpleExecutionQueue(
    "pruning-processor-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  reportUnfinishedPruning()(TraceContext.empty)

  /** Logs a warning if there is an unfinished pruning.
    */
  private def reportUnfinishedPruning()(implicit traceContext: TraceContext): Unit =
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      executionQueue
        .executeUS(
          for {
            status <- participantNodePersistentState.value.pruningStore.pruningStatus()
          } yield {
            if (status.isInProgress)
              logger.warn(
                show"Unfinished pruning operation. The participant has been partially pruned up to ${status.startedO.showValue}. " +
                  show"The last successful pruning operation has deleted all events up to ${status.completedO.showValueOrNone}."
              )
            else logger.info(show"Pruning status: $status")
          },
          functionFullName,
        ),
      "Unable to retrieve pruning status.",
      level = if (isClosing) Level.INFO else Level.ERROR,
    )

  /** Prune ledger event stream of this participant up to the given global offset inclusively.
    * Returns the global offset of the last pruned event.
    *
    * Safe to call multiple times concurrently.
    */
  def pruneLedgerEvents(
      pruneUpToInclusive: Offset
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = {

    def go(lastUpTo: Option[Offset]): FutureUnlessShutdown[
      Either[Option[Offset], Either[LedgerPruningError, Unit]]
    ] = {
      val pruneUpToNext = increaseByBatchSize(lastUpTo)
      val offset = pruneUpToNext.min(pruneUpToInclusive)
      val done = offset == pruneUpToInclusive
      pruneLedgerEventBatch(lastUpTo, offset).transform {
        case Left(e) => Right(Left(e))
        case Right(_) if done => Right(Either.unit)
        case Right(_) => Left(Some(offset))
      }.value
    }

    def doPrune()(implicit
        executionContext: ExecutionContext
    ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] =
      EitherTUtil.timed(metrics.overall)(
        for {
          pruningStatus <- EitherT
            .right(
              participantNodePersistentState.value.pruningStore.pruningStatus()
            )
          _ensuredSafeToPrune <- ensurePruningOffsetIsSafe(pruneUpToInclusive)
          _prunedAllEventBatches <- EitherT(
            Monad[FutureUnlessShutdown].tailRecM(pruningStatus.completedO)(go)
          )
        } yield ()
      )
    executionQueue.executeEUS(doPrune(), s"prune ledger events upto $pruneUpToInclusive")
  }

  /** Returns an offset of at most `boundInclusive` that is safe to prune and whose timestamp is
    * before or at `beforeOrAt`.
    *
    * @param boundInclusive
    *   The caller must choose a bound so that the ledger API server never requests an offset at or
    *   below `boundInclusive`. Offsets at or below ledger end are typically a safe choice.
    */
  def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: Offset)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[Offset]] = EitherT(
    participantNodePersistentState.value.ledgerApiStore
      .lastSynchronizerOffsetBeforeOrAtPublicationTime(beforeOrAt)
      .map(_.map(_.offset))
      .flatMap {
        case Some(beforeOrAtOffset) =>
          // under the hood this computation not only pushes back the boundInclusive bound according to the beforeOrAt publication timestamp, but also pushes it back before the ledger-end
          val rewoundBoundInclusive: Offset =
            if (beforeOrAtOffset >= boundInclusive) boundInclusive else beforeOrAtOffset
          firstUnsafeOffset(
            syncPersistentStateManager.getAll.toList,
            rewoundBoundInclusive,
          ).map(
            _.map(_.offset)
              .flatMap(_.decrement)
              .filter(_ < rewoundBoundInclusive)
              .orElse(Some(rewoundBoundInclusive))
          ).value

        case None =>
          FutureUnlessShutdown.pure(
            Left(LedgerPruningNothingToPrune)
          ) // nothing to prune, beforeOrAt is too low
      }
  )

  /** Purge all data of the specified synchronizer that must be inactive.
    */
  def purgeInactiveSynchronizer(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = for {
    persistenceState <- EitherT.fromEither[FutureUnlessShutdown](
      syncPersistentStateManager
        .get(synchronizerId)
        .toRight(PurgingUnknownSynchronizer(synchronizerId))
    )
    synchronizerStatus <- EitherT.fromEither[FutureUnlessShutdown](
      synchronizerConnectionStatus(synchronizerId).toRight(
        PurgingUnknownSynchronizer(synchronizerId)
      )
    )
    _ <- EitherT.cond[FutureUnlessShutdown](
      synchronizerStatus == SynchronizerConnectionConfigStore.Inactive,
      (),
      PurgingOnlyAllowedOnInactiveSynchronizer(synchronizerId, synchronizerStatus),
    )
    _ = logger.info(s"Purging inactive synchronizer $synchronizerId")

    _ <- EitherT.right(
      performUnlessClosingUSF("Purge inactive synchronizer")(purgeSynchronizer(persistenceState))
    )
  } yield ()

  private def firstUnsafeOffset(
      allSynchronizers: List[(SynchronizerId, SyncPersistentState)],
      pruneUptoInclusive: Offset,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {

    def firstUnsafeEventFor(
        synchronizerId: SynchronizerId,
        persistent: SyncPersistentState,
    ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        synchronizerIndex <- EitherT
          .right(
            participantNodePersistentState.value.ledgerApiStore
              .cleanSynchronizerIndex(synchronizerId)
          )
        sortedReconciliationIntervalsProvider <- sortedReconciliationIntervalsProviderFactory
          .get(
            synchronizerId,
            synchronizerIndex
              .flatMap(_.sequencerIndex)
              .map(_.timestamp)
              .getOrElse(CantonTimestamp.MinValue),
          )
          .leftMap(LedgerPruningInternalError.apply)

        safeCommitmentTick <- EitherT
          .fromOptionF[FutureUnlessShutdown, LedgerPruningError, CantonTimestampSecond](
            PruningProcessor.latestSafeToPruneTick(
              persistent.requestJournalStore,
              synchronizerIndex,
              sortedReconciliationIntervalsProvider,
              persistent.acsCommitmentStore,
              participantNodePersistentState.value.inFlightSubmissionStore,
              synchronizerId,
              checkForOutstandingCommitments = true,
            ),
            Pruning.LedgerPruningOffsetUnsafeSynchronizer(synchronizerId),
          )
        _ = logger.debug(
          s"Safe commitment tick for synchronizer $synchronizerId at $safeCommitmentTick"
        )

        // Topology event crash recovery requires to not prune above the earliest sequenced timestamp referring to a not yet effective topology transaction,
        // as SequencedEventStore is used to get the trace-context of the originating topology-transaction.
        earliestSequencedTimestampForNonEffectiveTopologyTransactions <-
          synchronizerIndex
            .map(_.recordTime)
            .map(recordTime =>
              EitherT.right(
                persistent.topologyStore
                  .findEffectiveStateChanges(
                    fromEffectiveInclusive =
                      recordTime, // as if we would crash at current SynchronizerIndex
                    onlyAtEffective = false,
                  ) // using the same query as in topology crash recovery
                  .map(_.view.map(_.sequencedTime).minOption.map(_.value))
              )
            )
            .getOrElse(EitherT.right(FutureUnlessShutdown.pure(None)))
        _ = logger.debug(
          s"Earliest sequenced timestamp for not-yet-effective topology transactions for synchronizer $synchronizerId: $earliestSequencedTimestampForNonEffectiveTopologyTransactions"
        )

        (firstUnsafeRecordTime, cause) =
          earliestSequencedTimestampForNonEffectiveTopologyTransactions
            .filter(_ < safeCommitmentTick.forgetRefinement)
            .map(_ -> "Topology event crash recovery")
            .getOrElse(
              safeCommitmentTick.forgetRefinement -> "ACS background reconciliation and crash recovery"
            )

        firstUnsafeOffsetO <- EitherT
          .right(
            participantNodePersistentState.value.ledgerApiStore.firstSynchronizerOffsetAfterOrAt(
              synchronizerId,
              firstUnsafeRecordTime,
            )
          )
      } yield {
        logger.debug(
          s"First unsafe pruning offset for synchronizer $synchronizerId at $firstUnsafeOffsetO"
        )
        firstUnsafeOffsetO.map(synchronizerOffset =>
          UnsafeOffset(
            offset = synchronizerOffset.offset,
            synchronizerId = synchronizerId,
            recordTime = CantonTimestamp(synchronizerOffset.recordTime),
            cause = cause,
          )
        )
      }

    def firstUnsafeReassignmentEventFor(
        synchronizerId: SynchronizerId,
        persistent: SyncPersistentState,
    ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        earliestIncompleteReassignmentO <- EitherT
          .right(
            persistent.reassignmentStore.findEarliestIncomplete()
          )

        unsafeOffset <- earliestIncompleteReassignmentO.fold(
          EitherT.rightT[FutureUnlessShutdown, LedgerPruningError](None: Option[UnsafeOffset])
        ) { earliestIncompleteReassignment =>
          val (
            earliestIncompleteReassignmentGlobalOffset,
            earliestIncompleteReassignmentId,
            targetSynchronizerId,
          ) = earliestIncompleteReassignment
          for {
            unsafeOffsetForReassignments <- EitherT(
              participantNodePersistentState.value.ledgerApiStore
                .synchronizerOffset(earliestIncompleteReassignmentGlobalOffset)
                .map(
                  _.toRight(
                    Pruning.LedgerPruningInternalError(
                      s"incomplete reassignment from $earliestIncompleteReassignmentGlobalOffset not found on $synchronizerId"
                    ): LedgerPruningError
                  )
                )
            )
            unsafeOffsetEarliestIncompleteReassignmentO = Option(
              UnsafeOffset(
                unsafeOffsetForReassignments.offset,
                unsafeOffsetForReassignments.synchronizerId,
                CantonTimestamp(unsafeOffsetForReassignments.recordTime),
                s"incomplete reassignment from ${earliestIncompleteReassignmentId.sourceSynchronizer} to $targetSynchronizerId (reassignmentId $earliestIncompleteReassignmentId)",
              )
            )

          } yield unsafeOffsetEarliestIncompleteReassignmentO
        }
      } yield {
        logger.debug(
          s"First unsafe pruning offset for synchronizer $synchronizerId at $unsafeOffset"
        )
        unsafeOffset
      }

    // Make sure that we do not prune an offset whose publication time has not been elapsed since the max deduplication duration.
    def firstUnsafeOffsetPublicationTime: FutureUnlessShutdown[Option[UnsafeOffset]] = {
      val (dedupStartLowerBound, maxDedupDuration) =
        participantNodePersistentState.value.settingsStore.settings.maxDeduplicationDuration match {
          case None =>
            // If we don't know the max dedup duration, use the earliest possible timestamp to be on the safe side
            CantonTimestamp.MinValue -> "unknown"
          case Some(maxDedupDuration) =>
            // Take the highest publication time of a published event as the baseline for converting the duration,
            // because the `CommandDeduplicator` will not use a lower timestamp, even if the participant clock
            // jumps backwards during fail-over.
            val publicationTimeLowerBound =
              participantNodePersistentState.value.ledgerApiStore
                .ledgerEndCache()
                .map(_.lastPublicationTime)
                .getOrElse(CantonTimestamp.MinValue)
            logger.debug(
              s"Publication time lower bound is $publicationTimeLowerBound with max deduplication duration of $maxDedupDuration"
            )
            // Subtract on `java.time.Instant` instead of CantonTimestamp so that we don't error on an underflow
            CantonTimestamp
              .fromInstant(publicationTimeLowerBound.toInstant.minus(maxDedupDuration.unwrap))
              .getOrElse(CantonTimestamp.MinValue) ->
              show"${maxDedupDuration.duration}"
        }
      participantNodePersistentState.value.ledgerApiStore
        .firstSynchronizerOffsetAfterOrAtPublicationTime(dedupStartLowerBound)
        .map(
          _.map(synchronizerOffset =>
            UnsafeOffset(
              offset = synchronizerOffset.offset,
              synchronizerId = synchronizerOffset.synchronizerId,
              recordTime = CantonTimestamp(synchronizerOffset.recordTime),
              cause = s"max deduplication duration of $maxDedupDuration",
            )
          )
        )
    }

    val allActiveSynchronizersE = {
      // Check that no migration is running concurrently.
      // This is just a sanity check; it does not prevent a migration from being started concurrently with pruning
      import SynchronizerConnectionConfigStore.*
      allSynchronizers.filterA { case (synchronizerId, _state) =>
        synchronizerConnectionStatus(synchronizerId) match {
          case None =>
            Left(LedgerPruningInternalError(s"No synchronizer status for $synchronizerId"))
          case Some(Active) => Right(true)
          case Some(Inactive) => Right(false)
          case Some(migratingStatus) =>
            logger.warn(
              s"Unable to prune while $synchronizerId is being migrated ($migratingStatus)"
            )
            Left(LedgerPruningNotPossibleDuringHardMigration(synchronizerId, migratingStatus))
        }
      }
    }
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        participantNodePersistentState.value.ledgerApiStore
          .ledgerEndCache()
          .map(_.lastOffset)
          >= Some(pruneUptoInclusive),
        (),
        Pruning.LedgerPruningOffsetAfterLedgerEnd: LedgerPruningError,
      )
      allActiveSynchronizers <- EitherT.fromEither[FutureUnlessShutdown](allActiveSynchronizersE)
      affectedSynchronizerOffsets <- EitherT
        .right[LedgerPruningError](allActiveSynchronizers.parFilterA {
          case (synchronizerId, _persistent) =>
            participantNodePersistentState.value.ledgerApiStore
              .lastSynchronizerOffsetBeforeOrAt(synchronizerId, pruneUptoInclusive)
              .map(_.isDefined)
        })
      _ <- EitherT.cond[FutureUnlessShutdown](
        affectedSynchronizerOffsets.nonEmpty,
        (),
        LedgerPruningNothingToPrune: LedgerPruningError,
      )
      unsafeSynchronizerOffsets <- affectedSynchronizerOffsets.parTraverseFilter {
        case (synchronizerId, persistent) =>
          firstUnsafeEventFor(synchronizerId, persistent)
      }
      unsafeIncompleteReassignmentOffsets <- allSynchronizers.parTraverseFilter {
        case (synchronizerId, persistent) =>
          firstUnsafeReassignmentEventFor(synchronizerId, persistent)
      }
      unsafeDedupOffset <- EitherT
        .right(firstUnsafeOffsetPublicationTime)
    } yield (unsafeDedupOffset.toList ++ unsafeSynchronizerOffsets ++ unsafeIncompleteReassignmentOffsets)
      .minByOption(_.offset)
  }

  private def pruneLedgerEventBatch(
      lastUpTo: Option[Offset],
      pruneUpToInclusiveBatchEnd: Offset,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] =
    performUnlessClosingEitherUSF[LedgerPruningError, Unit](functionFullName) {
      logger.info(s"Start pruning up to $pruneUpToInclusiveBatchEnd...")
      val pruningStore = participantNodePersistentState.value.pruningStore
      for {
        _ <- EitherT.right(
          pruningStore.markPruningStarted(pruneUpToInclusiveBatchEnd)
        )
        _ <- EitherT.right(performPruning(lastUpTo, pruneUpToInclusiveBatchEnd))
        _ <- EitherT.right(
          pruningStore.markPruningDone(pruneUpToInclusiveBatchEnd)
        )
      } yield {
        logger.info(s"Pruned up to $pruneUpToInclusiveBatchEnd")
      }
    }

  private def lookUpSynchronizerAndParticipantPruningCutoffs(
      pruneFromExclusive: Option[Offset],
      pruneUpToInclusive: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PruningCutoffs] =
    for {
      lastOffsetBeforeOrAtPruneUptoInclusive <- participantNodePersistentState.value.ledgerApiStore
        .lastSynchronizerOffsetBeforeOrAt(pruneUpToInclusive)
      lastOffsetInPruningRange = lastOffsetBeforeOrAtPruneUptoInclusive
        .filter(synchronizerOffset => Option(synchronizerOffset.offset) > pruneFromExclusive)
        .map(synchronizerOffset =>
          (
            synchronizerOffset.offset,
            CantonTimestamp(synchronizerOffset.publicationTime),
          )
        )
      synchronizerOffsets <- syncPersistentStateManager.getAll.toList.parTraverseFilter {
        case (synchronizerId, state) =>
          participantNodePersistentState.value.ledgerApiStore
            .lastSynchronizerOffsetBeforeOrAt(synchronizerId, pruneUpToInclusive)
            .flatMap(
              _.filter(synchronizerOffset => Option(synchronizerOffset.offset) > pruneFromExclusive)
                .map(synchronizerOffset =>
                  state.requestJournalStore
                    .lastRequestTimeWithRequestTimestampBeforeOrAt(
                      CantonTimestamp(synchronizerOffset.recordTime)
                    )
                    .map(timeOfRequestO =>
                      Some(
                        PruningCutoffs.SynchronizerOffset(
                          state = state,
                          lastTimestamp = CantonTimestamp(synchronizerOffset.recordTime),
                          lastRequestCounter = timeOfRequestO.map(_.rc),
                        )
                      )
                    )
                )
                .getOrElse(FutureUnlessShutdown.pure(None))
            )
      }
    } yield PruningCutoffs(
      lastOffsetInPruningRange,
      synchronizerOffsets,
    )

  private def lookUpContractsArchivedBeforeOrAt(
      fromExclusive: Option[Offset],
      upToInclusive: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfContractId]] =
    participantNodePersistentState.value.ledgerApiStore.archivals(
      fromExclusive,
      upToInclusive,
    )

  private def ensurePruningOffsetIsSafe(
      offset: Offset
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = {

    val synchronizers = syncPersistentStateManager.getAll.toList
    for {
      firstUnsafeOffsetO <- firstUnsafeOffset(synchronizers, offset)
        // if nothing to prune we go on with this iteration regardless to ensure that iterative and scheduled pruning is not stuck in a window where nothing to prune
        .recover { case LedgerPruningNothingToPrune => None }
      _ <- firstUnsafeOffsetO match {
        case None => EitherT.pure[FutureUnlessShutdown, LedgerPruningError](())
        case Some(unsafe) if unsafe.offset > offset =>
          EitherT.pure[FutureUnlessShutdown, LedgerPruningError](())
        case Some(unsafe) =>
          EitherT
            .leftT[FutureUnlessShutdown, Unit]
            .apply[LedgerPruningError](
              Pruning.LedgerPruningOffsetUnsafeToPrune(
                offset,
                unsafe.synchronizerId,
                unsafe.recordTime,
                unsafe.cause,
                unsafe.offset.decrement,
              )
            )
      }
    } yield ()
  }

  private[pruning] def performPruning(
      fromExclusive: Option[Offset],
      upToInclusive: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      cutoffs <- lookUpSynchronizerAndParticipantPruningCutoffs(fromExclusive, upToInclusive)

      archivedContracts <- lookUpContractsArchivedBeforeOrAt(fromExclusive, upToInclusive)

      // We must prune the contract store even if the event log is empty, because there is not necessarily an
      // archival event reassigned-away contracts.
      _ = logger.debug("Pruning contract store...")
      _ <- participantNodePersistentState.value.contractStore.deleteIgnoringUnknown(
        archivedContracts
      )

      _ <- cutoffs.synchronizerOffsets.parTraverse(pruneSynchronizer)
      _ <- cutoffs.globalOffsetO.fold(FutureUnlessShutdown.unit) {
        case (globalOffset, publicationTime) =>
          pruneDeduplicationStore(globalOffset, publicationTime)
      }
    } yield ()

  /** Prune a synchronizer persistent state.
    */
  private def pruneSynchronizer(synchronizerOffset: PruningCutoffs.SynchronizerOffset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val PruningCutoffs.SynchronizerOffset(state, lastTimestamp, lastRequestCounter) =
      synchronizerOffset

    logger.info(
      show"Pruning ${state.indexedSynchronizer.synchronizerId} up to $lastTimestamp and request counter $lastRequestCounter"
    )

    // we don't prune stores that are pruned by the JournalGarbageCollector regularly anyway
    logger.debug("Pruning sequenced event store...")

    for {
      _ <- state.sequencedEventStore.prune(lastTimestamp)

      _ = logger.debug("Pruning request journal store...")
      _ <- state.requestJournalStore.prune(lastTimestamp)

      _ = logger.debug("Pruning acs commitment store...")
      _ <- state.acsCommitmentStore.prune(lastTimestamp)
      // TODO(#2600) Prune the reassignment store
    } yield ()
  }

  private def purgeSynchronizer(state: SyncPersistentState)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.info(s"Purging synchronizer ${state.indexedSynchronizer.synchronizerId}")

    logger.debug("Purging active contract store...")
    for {
      // Purge stores that are pruned by the ConnectedSynchronizer's JournalGarbageCollector as the ConnectedSynchronizer
      // is never active anymore.
      _ <- state.activeContractStore.purge()

      _ = logger.debug("Purging sequenced event store...")
      _ <- state.sequencedEventStore.purge()

      _ = logger.debug("Purging request journal store...")
      _ <- state.requestJournalStore.purge()

      // We don't purge the ACS commitment store, as the data might still serve as audit evidence.

      _ = logger.debug("Purging submission tracker store...")
      _ <- state.submissionTrackerStore.purge()

      // TODO(#2600) Purge the reassignment store when implementing pruning
    } yield {
      logger.info(
        s"Purging synchronizer ${state.indexedSynchronizer.synchronizerId} has been completed"
      )
    }
  }

  private def pruneDeduplicationStore(
      globalOffset: Offset,
      publicationTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Pruning command deduplication table at $globalOffset with publication time $publicationTime..."
    )
    participantNodePersistentState.value.commandDeduplicationStore
      .prune(globalOffset, publicationTime)
  }

  override protected def onClosed(): Unit = LifeCycle.close(executionQueue)(logger)

  def acsSetNoWaitCommitmentsFrom(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .addNoWaitCounterParticipant(configs)

  def acsGetNoWaitCommitmentsFrom(
      synchronizers: Seq[SynchronizerId],
      participants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]] =
    for {
      allNoWait <- participantNodePersistentState.value.acsCounterParticipantConfigStore
        .getAllActiveNoWaitCounterParticipants(
          synchronizers,
          participants,
        )
    } yield allNoWait

  def acsResetNoWaitCommitmentsFrom(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .removeNoWaitCounterParticipant(configs.map(_.synchronizerId), configs.map(_.participantId))

  /** Providing the next Offset for iterative pruning: computed by the current pruning Offset
    * increased by the max pruning batch size.
    */
  def locatePruningOffsetForOneIteration(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Offset] =
    EitherT
      .right(
        participantNodePersistentState.value.pruningStore.pruningStatus()
      )
      .map(_.completedO)
      .map(increaseByBatchSize)

  private def increaseByBatchSize(offset: Option[Offset]): Offset =
    Offset.tryFromLong(offset.fold(0L)(_.unwrap) + maxPruningBatchSize.value)

}

private[pruning] object PruningProcessor extends HasLoggerName {

  /* Extracted to be able to test more easily */
  @VisibleForTesting
  private[pruning] def safeToPrune_(
      cleanReplayF: FutureUnlessShutdown[CantonTimestamp],
      commitmentsPruningBound: CommitmentsPruningBound,
      earliestInFlightSubmissionFUS: FutureUnlessShutdown[Option[CantonTimestamp]],
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      synchronizerId: SynchronizerId,
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] =
    for {
      // This logic progressively lowers the timestamp based on the following constraints:
      // 1. Pruning must not delete data needed for recovery (after the clean replay timestamp)
      cleanReplayTs <- cleanReplayF

      // 2. Pruning must not delete events from the event log for which there are still in-flight submissions.
      // We check here the synchronizer related events only.
      //
      // Processing of sequenced events may concurrently move the earliest in-flight submission back in time
      // (from timeout to sequencing timestamp), but this can only happen if the corresponding request is not yet clean,
      // i.e., the sequencing timestamp is after `cleanReplayTs`. So this concurrent modification does not affect
      // the calculation below.
      inFlightSubmissionTs <- earliestInFlightSubmissionFUS

      getTickBeforeOrAt = (ts: CantonTimestamp) =>
        sortedReconciliationIntervalsProvider
          .reconciliationIntervals(ts)(loggingContext.traceContext)
          .map(_.tickBeforeOrAt(ts))
          .flatMap {
            case Some(tick) =>
              loggingContext.debug(
                s"Tick before or at $ts yields $tick on synchronizer $synchronizerId"
              )
              FutureUnlessShutdown.pure(tick)
            case None =>
              FutureUnlessShutdown.failed(
                new RuntimeException(
                  s"Unable to compute tick before or at `$ts` for synchronizer $synchronizerId"
                )
              )
          }

      // Latest potential pruning point is the ACS commitment tick before or at the "clean replay" timestamp
      // and strictly before the earliest timestamp associated with an in-flight submission.
      latestTickBeforeOrAt <- getTickBeforeOrAt(
        cleanReplayTs.min(
          inFlightSubmissionTs.fold(CantonTimestamp.MaxValue)(_.immediatePredecessor)
        )
      )

      // Only acs commitment ticks whose ACS commitment fully matches all counter participant ACS commitments are safe,
      // so look for the most recent such tick before latestTickBeforeOrAt if any.
      tsSafeToPruneUpTo <- commitmentsPruningBound match {
        case CommitmentsPruningBound.Outstanding(noOutstandingCommitmentsF) =>
          noOutstandingCommitmentsF(latestTickBeforeOrAt.forgetRefinement)
            .flatMap(
              _.traverse(getTickBeforeOrAt)
            )
        case CommitmentsPruningBound.LastComputedAndSent(lastComputedAndSentF) =>
          for {
            lastComputedAndSentO <- lastComputedAndSentF
            tickBeforeLastComputedAndSentO <- lastComputedAndSentO.traverse(getTickBeforeOrAt)
          } yield tickBeforeLastComputedAndSentO.map(_.min(latestTickBeforeOrAt))
      }

      _ = loggingContext.debug {
        val timestamps = Map(
          "cleanReplayTs" -> cleanReplayTs.toString,
          "inFlightSubmissionTs" -> inFlightSubmissionTs.toString,
          "latestTickBeforeOrAt" -> latestTickBeforeOrAt.toString,
          "tsSafeToPruneUpTo" -> tsSafeToPruneUpTo.toString,
        )

        s"Getting safe to prune commitment tick with data $timestamps on synchronizer $synchronizerId"
      }

      // Sanity check that safe pruning timestamp has not "increased" (which would be a coding bug).
      _ = tsSafeToPruneUpTo.foreach(ts =>
        ErrorUtil.requireState(
          ts <= latestTickBeforeOrAt,
          s"limit $tsSafeToPruneUpTo after $latestTickBeforeOrAt on synchronizer $synchronizerId",
        )
      )
    } yield tsSafeToPruneUpTo

  /** The latest commitment tick before or at the given time at which it is safe to prune. */
  def latestSafeToPruneTick(
      requestJournalStore: RequestJournalStore,
      synchronizerIndexO: Option[SynchronizerIndex],
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      acsCommitmentStore: AcsCommitmentStore,
      inFlightSubmissionStore: InFlightSubmissionStore,
      synchronizerId: SynchronizerId,
      checkForOutstandingCommitments: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val cleanReplayF = SyncEphemeralStateFactory
      .crashRecoveryPruningBoundInclusive(requestJournalStore, synchronizerIndexO)

    val commitmentsPruningBound =
      if (checkForOutstandingCommitments)
        CommitmentsPruningBound.Outstanding(ts => acsCommitmentStore.noOutstandingCommitments(ts))
      else
        CommitmentsPruningBound.LastComputedAndSent(
          acsCommitmentStore.lastComputedAndSent.map(_.map(_.forgetRefinement))
        )

    val earliestInFlightF = inFlightSubmissionStore.lookupEarliest(synchronizerId)

    safeToPrune_(
      cleanReplayF,
      commitmentsPruningBound = commitmentsPruningBound,
      earliestInFlightF,
      sortedReconciliationIntervalsProvider,
      synchronizerId,
    )
  }
  private final case class UnsafeOffset(
      offset: Offset,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      cause: String,
  )

  /** PruningCutoffs captures two "formats" of the same pruning cutoff: The global offset and
    * per-synchronizer local offsets (with participant offset).
    * @param synchronizerOffsets
    *   cutoff as synchronizer-local offsets used for canton-internal per-synchronizer pruning
    */
  final case class PruningCutoffs(
      globalOffsetO: Option[(Offset, CantonTimestamp)],
      synchronizerOffsets: List[PruningCutoffs.SynchronizerOffset],
  )

  object PruningCutoffs {

    /** @param state
      *   SyncsyPersistentState of the synchronizer
      * @param lastTimestamp
      *   Last sequencing timestamp below the given globalOffset
      * @param lastRequestCounter
      *   Last request counter below the given globalOffset
      */
    final case class SynchronizerOffset(
        state: SyncPersistentState,
        lastTimestamp: CantonTimestamp,
        lastRequestCounter: Option[RequestCounter],
    )
  }
}
