// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, Offset}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.Pruning
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
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
    sortedReconciliationIntervalsProviderFactory: SortedReconciliationIntervalsProviderFactory,
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {
  import PruningProcessor.*

  /*
  TODO(#24716) considering the guarantees that we have, it probably does not make sense to have this physical
  We can probably do one query per logical synchronizer
   */
  def perform(
      allSynchronizers: Seq[SyncPersistentState],
      pruneUptoInclusive: Offset,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {

    // Keep only one persistent state per logical id
    // TODO(#24716) See comment at the the of the class. This should be revisited.
    val synchronizers: Map[SynchronizerId, SyncPersistentState] = allSynchronizers
      .groupBy1(_.psid)
      .map { case (psid, states) => psid.logical -> states.maxBy1(_.psid) }

    val allActiveSynchronizersE = {
      // Check that no migration is running concurrently.
      // This is just a sanity check; it does not prevent a migration from being started concurrently with pruning
      import SynchronizerConnectionConfigStore.*
      synchronizers.toList.filterA { case (synchronizerId, _state) =>
        synchronizerConnectionConfigStore.getAllStatusesFor(synchronizerId) match {
          case Left(_: UnknownId) =>
            Left(LedgerPruningInternalError(s"No synchronizer status for $synchronizerId"))
          case Right(configs) =>
            configs.forgetNE
              .traverse {
                case Active => Right(true)
                case Inactive => Right(false)
                case migratingStatus =>
                  logger.warn(
                    s"Unable to prune while $synchronizerId is being migrated ($migratingStatus)"
                  )
                  Left(LedgerPruningNotPossibleDuringHardMigration(synchronizerId, migratingStatus))
              }
              // Considered active is there is one active connection
              .map(_.exists(identity))
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
      // TODO(#26490) Do we need the synchronizer id in the tuple knowing it is in the persistent state?
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
        case (_, persistent) => firstUnsafeEventFor(persistent)
      }
      unsafeIncompleteReassignmentOffsets <- synchronizers.toList.parTraverseFilter {
        case (_, persistent) => firstUnsafeReassignmentEventFor(persistent)
      }
      unsafeDedupOffset <- EitherT.right(firstUnsafeOffsetPublicationTime())
    } yield (unsafeDedupOffset.toList ++ unsafeSynchronizerOffsets ++ unsafeIncompleteReassignmentOffsets)
      .minByOption(_.offset)
  }

  private def firstUnsafeEventFor(
      persistent: SyncPersistentState
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    val synchronizerId = persistent.psid
    for {
      synchronizerIndex <- EitherT
        .right(
          participantNodePersistentState.value.ledgerApiStore
            .cleanSynchronizerIndex(synchronizerId.logical)
        )
      _ = logger.debug(
        s"SynchronizerIndex for synchronizer $synchronizerId: $synchronizerIndex "
      )

      sortedReconciliationIntervalsProvider <- sortedReconciliationIntervalsProviderFactory
        .get(
          synchronizerId,
          synchronizerIndex
            .flatMap(_.sequencerIndex)
            .map(_.sequencerTimestamp)
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
          Pruning.LedgerPruningOffsetUnsafeSynchronizer(synchronizerId.logical),
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
                  // as if we would crash at current SynchronizerIndex
                  fromEffectiveInclusive = recordTime,
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
        List(
          // The sequenced event should not be pruned for the clean sequencer index, as the sequencer counter is looked up from the SequencedEventStore.
          synchronizerIndex
            .flatMap(_.sequencerIndex)
            .map(_.sequencerTimestamp -> "Synchronizer index crash recover")
            .toList,
          earliestSequencedTimestampForNonEffectiveTopologyTransactions
            .map(_ -> "Topology event crash recovery")
            .toList,
        ).flatten
          .filter(_._1 < safeCommitmentTick.forgetRefinement)
          .minByOption(_._1)
          .getOrElse(
            safeCommitmentTick.forgetRefinement -> "ACS background reconciliation and crash recovery"
          )

      firstUnsafeOffsetO <- EitherT
        .right(
          participantNodePersistentState.value.ledgerApiStore.firstSynchronizerOffsetAfterOrAt(
            synchronizerId.logical,
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
          synchronizerId = synchronizerId.logical,
          recordTime = CantonTimestamp(synchronizerOffset.recordTime),
          cause = cause,
        )
      )
    }
  }

  private def firstUnsafeReassignmentEventFor(
      persistent: SyncPersistentState
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    val synchronizerId = persistent.psid.logical

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
          unsafeOffsetForReassignments <- EitherT[
            FutureUnlessShutdown,
            LedgerPruningError,
            SynchronizerOffset,
          ](
            participantNodePersistentState.value.ledgerApiStore
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
      logger.debug(
        s"First unsafe pruning offset for synchronizer $synchronizerId at $unsafeOffset"
      )
      unsafeOffset
    }
  }

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
