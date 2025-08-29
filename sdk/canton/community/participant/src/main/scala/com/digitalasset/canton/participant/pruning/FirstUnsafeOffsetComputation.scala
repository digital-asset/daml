// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.Pruning
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.pruning.PruningProcessor.UnsafeOffset
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
  HardMigratingSource,
  HardMigratingTarget,
  Inactive,
  UnknownId,
  UpgradingTarget,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

/** The pruning processor coordinates the pruning of all participant node stores
  *
  * @param participantNodePersistentState
  *   the persistent state of the participant node that is not specific to a synchronizer
  */
/*
 TODO(#24716) This class should be revisited to check physical <> logical.
 Split physical and logical pruning. In particular, many pruning computations are logical
 (because they relate to contracts) but some of them are not (e.g., because they use the
 sequenced event store).
 */
class FirstUnsafeOffsetComputation(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    syncPersistentStateManager: SyncPersistentStateManager,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {
  import PruningProcessor.*

  private def activeSynchronizers()(implicit
      traceContext: TraceContext
  ): Either[LedgerPruningError, Seq[SyncPersistentState]] = {

    val synchronizers = synchronizerConnectionConfigStore.aliasResolution.logicalSynchronizerIds

    synchronizers.toList.traverseFilter { synchronizerId =>
      synchronizerConnectionConfigStore.getActive(synchronizerId) match {
        case Left(error) =>
          logger.info(
            s"Could not get active synchronizer config for $synchronizerId: ${error.message}"
          )
          Right(None)
        case Right(config) =>
          // We found an active config. Now check that no migration is running concurrently.
          // This is just a sanity check; it does not prevent a migration from being started concurrently with pruning
          checkForNoOngoingMigrationForSynchronizer(synchronizerId).flatMap { _ =>
            config.configuredPSId.toOption
              .flatMap(syncPersistentStateManager.get)
              .toRight(
                LedgerPruningInternalError(
                  s"Could not find persistent state for active synchronizer $synchronizerId"
                )
              )
              .map(Some(_))
          }
      }
    }
  }

  private def checkForNoOngoingMigrationForSynchronizer(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext) =
    synchronizerConnectionConfigStore.getAllStatusesFor(synchronizerId) match {
      case Left(_: UnknownId) =>
        Left[LedgerPruningError, Seq[SyncPersistentState]](
          LedgerPruningInternalError(s"No synchronizer status for $synchronizerId")
        )
      case Right(configs) =>
        configs.forgetNE
          .traverse_ {
            case Active | Inactive => Right(())
            case migratingStatus @ (HardMigratingSource | HardMigratingTarget | UpgradingTarget) =>
              logger.info(
                s"Unable to prune while $synchronizerId is being migrated ($migratingStatus)"
              )
              Left(LedgerPruningNotPossibleDuringHardMigration(synchronizerId, migratingStatus))
          }
    }

  /*
  TODO(#24716) considering the guarantees that we have, it probably does not make sense to have this physical
  We can probably do one query per logical synchronizer
   */
  def perform(
      pruneUptoInclusive: Offset
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] =
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        participantNodePersistentState.value.ledgerApiStore
          .ledgerEndCache()
          .map(_.lastOffset)
          >= Some(pruneUptoInclusive),
        (),
        Pruning.LedgerPruningOffsetAfterLedgerEnd: LedgerPruningError,
      )
      allSynchronizerPersistentStates <- EitherT.fromEither[FutureUnlessShutdown](
        activeSynchronizers()
      )
      pruningCandidatePersistentStates <- EitherT
        .right[LedgerPruningError](allSynchronizerPersistentStates.parFilterA { persistent =>
          participantNodePersistentState.value.ledgerApiStore
            .lastSynchronizerOffsetBeforeOrAt(persistent.lsid, pruneUptoInclusive)
            .map(_.isDefined)
        })
      _ <- EitherT.cond[FutureUnlessShutdown](
        pruningCandidatePersistentStates.nonEmpty,
        (),
        LedgerPruningNothingToPrune: LedgerPruningError,
      )
      pruningCandidatesWithSynchronizerIndex <-
        EitherT
          .right(
            MonadUtil
              .sequentialTraverse(pruningCandidatePersistentStates)(syncState =>
                participantNodePersistentState.value.ledgerApiStore
                  .cleanSynchronizerIndex(syncState.lsid)
                  .map { synchronizerIndex =>
                    errorLoggingContext.debug(
                      s"SynchronizerIndex for synchronizer ${syncState.lsid}: $synchronizerIndex "
                    )
                    syncState -> synchronizerIndex
                  }
              )
          )
      unsafeLogicalSynchronizerOffsets <- pruningCandidatesWithSynchronizerIndex.parTraverseFilter {
        case (syncPersistentState, synchronizerIndex) =>
          FirstUnsafeOffsetComputation.firstUnsafeLogicalOffset(
            syncPersistentState,
            synchronizerIndex,
            participantNodePersistentState.value.ledgerApiStore,
            participantNodePersistentState.value.inFlightSubmissionStore,
          )
      }
      unsafePhysicalSynchronizerOffsets <- pruningCandidatesWithSynchronizerIndex
        .parTraverseFilter { case (syncPersistentState, synchronizerIndex) =>
          FirstUnsafeOffsetComputation.firstUnsafePhysicalOffset(
            syncPersistentState,
            synchronizerIndex,
            participantNodePersistentState.value.ledgerApiStore,
          )
        }
      unsafeIncompleteReassignmentOffsets <- allSynchronizerPersistentStates.parTraverseFilter(
        FirstUnsafeOffsetComputation.firstUnsafeReassignmentEventFor(
          _,
          participantNodePersistentState.value.ledgerApiStore,
        )
      )
      unsafeDedupOffset <- EitherT.right(firstUnsafeOffsetPublicationTime())
    } yield (unsafeLogicalSynchronizerOffsets.toList ++ unsafeDedupOffset ++ unsafePhysicalSynchronizerOffsets ++ unsafeIncompleteReassignmentOffsets)
      .minByOption(_.offset)

  // Make sure that we do not prune an offset whose publication time has not been elapsed since the max deduplication duration.
  private def firstUnsafeOffsetPublicationTime()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[UnsafeOffset]] = {
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
}

object FirstUnsafeOffsetComputation {
  private def firstUnsafeReassignmentEventFor(
      persistent: LogicalSyncPersistentState,
      ledgerApiStore: LedgerApiStore,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    implicit val tc: TraceContext = errorLoggingContext.traceContext
    val synchronizerId = persistent.lsid

    for {
      earliestIncompleteReassignmentO <- EitherT
        .right(
          persistent.reassignmentStore.findEarliestIncomplete()
        )

      unsafeOffsetO <- earliestIncompleteReassignmentO.flatTraverse {
        case (
              earliestIncompleteReassignmentGlobalOffset,
              earliestIncompleteReassignmentId,
              targetSynchronizerId,
            ) =>
          for {
            unsafeOffsetForReassignments <- EitherT[
              FutureUnlessShutdown,
              LedgerPruningError,
              SynchronizerOffset,
            ](
              ledgerApiStore
                .synchronizerOffset(earliestIncompleteReassignmentGlobalOffset)
                .map(
                  _.toRight(
                    Pruning.LedgerPruningInternalError(
                      s"incomplete reassignment from $earliestIncompleteReassignmentGlobalOffset not found on $synchronizerId"
                    )
                  )
                )
            )
            unsafeOffsetEarliestIncompleteReassignmentO = Option(
              UnsafeOffset(
                unsafeOffsetForReassignments.offset,
                unsafeOffsetForReassignments.synchronizerId,
                CantonTimestamp(unsafeOffsetForReassignments.recordTime),
                s"incomplete reassignment from $synchronizerId to $targetSynchronizerId (reassignmentId $earliestIncompleteReassignmentId)",
              )
            )

          } yield unsafeOffsetEarliestIncompleteReassignmentO
      }
    } yield {
      errorLoggingContext.debug(
        s"First unsafe pruning offset from reassignment store for logical synchronizer $synchronizerId at $unsafeOffsetO"
      )
      unsafeOffsetO
    }
  }

  /** Determines the first offset that is unsafe to prune on the basis of logical synchronizer
    * stores, like inflight submission store or ledger api store.
    */
  private def firstUnsafeLogicalOffset(
      persistent: LogicalSyncPersistentState,
      synchronizerIndex: Option[SynchronizerIndex],
      ledgerApiStore: LedgerApiStore,
      inFlightSubmissionStore: InFlightSubmissionStore,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    implicit val tc: TraceContext = errorLoggingContext.traceContext
    val synchronizerId = persistent.lsid
    for {
      safeCommitmentTick <- EitherT
        .fromOptionF[FutureUnlessShutdown, LedgerPruningError, CantonTimestamp](
          persistent.acsCommitmentStore.noOutstandingCommitments(CantonTimestamp.MaxValue),
          Pruning.LedgerPruningOffsetUnsafeSynchronizer(synchronizerId),
        )
      _ = errorLoggingContext.debug(
        s"Safe commitment tick for synchronizer $synchronizerId: $safeCommitmentTick"
      )
      earliestInFlight <- EitherT.right(inFlightSubmissionStore.lookupEarliest(synchronizerId))

      unsafeTimestamps = NonEmpty(
        List,
        safeCommitmentTick -> "ACS background reconciliation",
      ) ++ synchronizerIndex
        .flatMap(_.sequencerIndex)
        .map(_.sequencerTimestamp -> "Synchronizer index crash recovery")
        ++ earliestInFlight.map(_ -> "inFlightSubmissionTs")

      _ = errorLoggingContext.debug(
        s"Getting safe to prune timestamp for logical synchronizer $synchronizerId with data ${unsafeTimestamps.forgetNE}"
      )

      (firstUnsafeTimestamp, cause) = unsafeTimestamps.minBy1(_._1)

      firstUnsafeOffsetO <- EitherT.right(
        ledgerApiStore.firstSynchronizerOffsetAfterOrAt(
          synchronizerId,
          firstUnsafeTimestamp,
        )
      )
    } yield {
      errorLoggingContext.debug(
        s"First unsafe pruning offset for logical synchronizer $synchronizerId at $firstUnsafeOffsetO from $cause"
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
  }

  /** Determines the first offset that is unsafe to prune on the basis of physical synchronizer
    * stores, like request journal store or topology store.
    */
  private def firstUnsafePhysicalOffset(
      persistent: PhysicalSyncPersistentState,
      synchronizerIndex: Option[SynchronizerIndex],
      ledgerApiStore: LedgerApiStore,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    implicit val tc: TraceContext = errorLoggingContext.traceContext
    val synchronizerId = persistent.psid
    for {
      crashRecovery <- EitherT.right(
        persistent.requestJournalStore.crashRecoveryPruningBoundInclusive(synchronizerIndex)
      )

      // Topology event crash recovery requires to not prune above the earliest sequenced timestamp referring to a not yet effective topology transaction,
      // as SequencedEventStore is used to get the trace-context of the originating topology-transaction.
      earliestSequencedTimestampForNonEffectiveTopologyTransactions <-
        synchronizerIndex
          .map(_.recordTime)
          .flatTraverse(recordTime =>
            EitherT.right[LedgerPruningError](
              persistent.topologyStore
                .findEffectiveStateChanges(
                  // as if we would crash at current SynchronizerIndex
                  fromEffectiveInclusive = recordTime,
                  onlyAtEffective = false,
                ) // using the same query as in topology crash recovery
                .map(_.view.map(_.sequencedTime).minOption.map(_.value))
            )
          )
      _ = errorLoggingContext.debug(
        s"Earliest sequenced timestamp for not-yet-effective topology transactions for synchronizer $synchronizerId: $earliestSequencedTimestampForNonEffectiveTopologyTransactions"
      )

      unsafeTimestamps = NonEmpty(
        List,
        crashRecovery -> "cleanReplayTs",
      ) ++ earliestSequencedTimestampForNonEffectiveTopologyTransactions
        .map(_ -> "Topology event crash recovery")

      (firstUnsafeRecordTime, cause) = unsafeTimestamps
        .minBy1(_._1)

      _ = errorLoggingContext.debug(
        s"Getting safe to prune timestamp for physical synchronizer $synchronizerId with data ${unsafeTimestamps.forgetNE}"
      )

      firstUnsafeOffsetO <- EitherT
        .right(
          ledgerApiStore.firstSynchronizerOffsetAfterOrAt(
            synchronizerId.logical,
            firstUnsafeRecordTime,
          )
        )
    } yield {
      errorLoggingContext.debug(
        s"First unsafe pruning offset for physical synchronizer $synchronizerId at $firstUnsafeOffsetO from $cause"
      )
      firstUnsafeOffsetO.map(synchronizerOffset =>
        UnsafeOffset(
          offset = synchronizerOffset.offset,
          synchronizerId = synchronizerId.logical,
          recordTime = CantonTimestamp(synchronizerOffset.recordTime),
          cause = cause,
        )
      )
    }
  }

}
