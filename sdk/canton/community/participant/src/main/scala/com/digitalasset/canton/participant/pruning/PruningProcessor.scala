// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.ledger.participant.state.DomainIndex
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
  DomainConnectionConfigStore,
  InFlightSubmissionStore,
  ParticipantNodePersistentState,
  RequestJournalStore,
  SyncDomainEphemeralStateFactory,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.pruning.ConfigForNoWaitCounterParticipants
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
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
  * @param participantNodePersistentState the persistent state of the participant node that is not specific to a domain
  * @param syncDomainPersistentStateManager domain state manager that provides access to domain-local stores for pruning
  * @param maxPruningBatchSize          size to which to break up pruning batches to limit (memory) resource consumption
  * @param metrics                      pruning metrics
  * @param exitOnFatalFailures          whether to crash on failures
  * @param domainConnectionStatus       helper to determine whether the domain is active or in another state
  */
class PruningProcessor(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    sortedReconciliationIntervalsProviderFactory: SortedReconciliationIntervalsProviderFactory,
    maxPruningBatchSize: PositiveInt,
    metrics: PruningMetrics,
    exitOnFatalFailures: Boolean,
    domainConnectionStatus: DomainId => Option[DomainConnectionConfigStore.Status],
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

  /** Returns an offset of at most `boundInclusive` that is safe to prune and whose timestamp is before or at `beforeOrAt`.
    *
    * @param boundInclusive The caller must choose a bound so that the ledger API server never requests an offset at or below `boundInclusive`.
    *                       Offsets at or below ledger end are typically a safe choice.
    */
  def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: Offset)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[Offset]] = EitherT(
    participantNodePersistentState.value.ledgerApiStore
      .lastDomainOffsetBeforeOrAtPublicationTime(beforeOrAt)
      .map(_.map(_.offset))
      .flatMap {
        case Some(beforeOrAtOffset) =>
          // under the hood this computation not only pushes back the boundInclusive bound according to the beforeOrAt publication timestamp, but also pushes it back before the ledger-end
          val rewoundBoundInclusive: Offset =
            if (beforeOrAtOffset >= boundInclusive) boundInclusive else beforeOrAtOffset
          firstUnsafeOffset(
            syncDomainPersistentStateManager.getAll.toList,
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

  /** Purge all data of the specified domain that must be inactive.
    */
  def purgeInactiveDomain(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = for {
    persistenceState <- EitherT.fromEither[FutureUnlessShutdown](
      syncDomainPersistentStateManager.get(domainId).toRight(PurgingUnknownDomain(domainId))
    )
    domainStatus <- EitherT.fromEither[FutureUnlessShutdown](
      domainConnectionStatus(domainId).toRight(PurgingUnknownDomain(domainId))
    )
    _ <- EitherT.cond[FutureUnlessShutdown](
      domainStatus == DomainConnectionConfigStore.Inactive,
      (),
      PurgingOnlyAllowedOnInactiveDomain(domainId, domainStatus),
    )
    _ = logger.info(s"Purging inactive domain $domainId")

    _ <- EitherT.right(
      performUnlessClosingUSF("Purge inactive domain")(purgeDomain(persistenceState))
    )
  } yield ()

  private def firstUnsafeOffset(
      allDomains: List[(DomainId, SyncDomainPersistentState)],
      pruneUptoInclusive: Offset,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {

    def firstUnsafeEventFor(
        domainId: DomainId,
        persistent: SyncDomainPersistentState,
    ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        domainIndex <- EitherT
          .right(
            participantNodePersistentState.value.ledgerApiStore
              .cleanDomainIndex(domainId)
          )
        sortedReconciliationIntervalsProvider <- sortedReconciliationIntervalsProviderFactory
          .get(
            domainId,
            domainIndex
              .flatMap(_.sequencerIndex)
              .map(_.timestamp)
              .getOrElse(CantonTimestamp.MinValue),
          )
          .leftMap(LedgerPruningInternalError.apply)
          .mapK(FutureUnlessShutdown.outcomeK)

        safeCommitmentTick <- EitherT
          .fromOptionF[FutureUnlessShutdown, LedgerPruningError, CantonTimestampSecond](
            PruningProcessor.latestSafeToPruneTick(
              persistent.requestJournalStore,
              domainIndex,
              sortedReconciliationIntervalsProvider,
              persistent.acsCommitmentStore,
              participantNodePersistentState.value.inFlightSubmissionStore,
              domainId,
              checkForOutstandingCommitments = true,
            ),
            Pruning.LedgerPruningOffsetUnsafeDomain(domainId),
          )
        _ = logger.debug(s"Safe commitment tick for domain $domainId at $safeCommitmentTick")

        firstUnsafeOffsetO <- EitherT
          .right(
            participantNodePersistentState.value.ledgerApiStore.firstDomainOffsetAfterOrAt(
              domainId,
              safeCommitmentTick.forgetRefinement,
            )
          )
      } yield {
        logger.debug(s"First unsafe pruning offset for domain $domainId at $firstUnsafeOffsetO")
        firstUnsafeOffsetO.map(domainOffset =>
          UnsafeOffset(
            offset = domainOffset.offset,
            domainId = domainId,
            recordTime = CantonTimestamp(domainOffset.recordTime),
            cause = s"ACS background reconciliation and crash recovery",
          )
        )
      }

    def firstUnsafeReassignmentEventFor(
        domainId: DomainId,
        persistent: SyncDomainPersistentState,
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
            targetDomainId,
          ) = earliestIncompleteReassignment
          for {
            unsafeOffsetForReassignments <- EitherT(
              participantNodePersistentState.value.ledgerApiStore
                .domainOffset(earliestIncompleteReassignmentGlobalOffset)
                .map(
                  _.toRight(
                    Pruning.LedgerPruningInternalError(
                      s"incomplete reassignment from $earliestIncompleteReassignmentGlobalOffset not found on $domainId"
                    ): LedgerPruningError
                  )
                )
            )
            unsafeOffsetEarliestIncompleteReassignmentO = Option(
              UnsafeOffset(
                unsafeOffsetForReassignments.offset,
                unsafeOffsetForReassignments.domainId,
                CantonTimestamp(unsafeOffsetForReassignments.recordTime),
                s"incomplete reassignment from ${earliestIncompleteReassignmentId.sourceDomain} to $targetDomainId (reassignmentId $earliestIncompleteReassignmentId)",
              )
            )

          } yield unsafeOffsetEarliestIncompleteReassignmentO
        }
      } yield {
        logger.debug(s"First unsafe pruning offset for domain $domainId at $unsafeOffset")
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
        .firstDomainOffsetAfterOrAtPublicationTime(dedupStartLowerBound)
        .map(
          _.map(domainOffset =>
            UnsafeOffset(
              offset = domainOffset.offset,
              domainId = domainOffset.domainId,
              recordTime = CantonTimestamp(domainOffset.recordTime),
              cause = s"max deduplication duration of $maxDedupDuration",
            )
          )
        )
    }

    val allActiveDomainsE = {
      // Check that no migration is running concurrently.
      // This is just a sanity check; it does not prevent a migration from being started concurrently with pruning
      import DomainConnectionConfigStore.*
      allDomains.filterA { case (domainId, _state) =>
        domainConnectionStatus(domainId) match {
          case None =>
            Left(LedgerPruningInternalError(s"No domain status for $domainId"))
          case Some(Active) => Right(true)
          case Some(Inactive) => Right(false)
          case Some(migratingStatus) =>
            logger.warn(s"Unable to prune while $domainId is being migrated ($migratingStatus)")
            Left(LedgerPruningNotPossibleDuringHardMigration(domainId, migratingStatus))
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
      allActiveDomains <- EitherT.fromEither[FutureUnlessShutdown](allActiveDomainsE)
      affectedDomainsOffsets <- EitherT
        .right[LedgerPruningError](allActiveDomains.parFilterA { case (domainId, _persistent) =>
          participantNodePersistentState.value.ledgerApiStore
            .lastDomainOffsetBeforeOrAt(domainId, pruneUptoInclusive)
            .map(_.isDefined)
        })
      _ <- EitherT.cond[FutureUnlessShutdown](
        affectedDomainsOffsets.nonEmpty,
        (),
        LedgerPruningNothingToPrune: LedgerPruningError,
      )
      unsafeDomainOffsets <- affectedDomainsOffsets.parTraverseFilter {
        case (domainId, persistent) =>
          firstUnsafeEventFor(domainId, persistent)
      }
      unsafeIncompleteReassignmentOffsets <- allDomains.parTraverseFilter {
        case (domainId, persistent) =>
          firstUnsafeReassignmentEventFor(domainId, persistent)
      }
      unsafeDedupOffset <- EitherT
        .right(firstUnsafeOffsetPublicationTime)
    } yield (unsafeDedupOffset.toList ++ unsafeDomainOffsets ++ unsafeIncompleteReassignmentOffsets)
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

  private def lookUpDomainAndParticipantPruningCutoffs(
      pruneFromExclusive: Option[Offset],
      pruneUpToInclusive: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PruningCutoffs] =
    for {
      lastOffsetBeforeOrAtPruneUptoInclusive <- participantNodePersistentState.value.ledgerApiStore
        .lastDomainOffsetBeforeOrAt(pruneUpToInclusive)
      lastOffsetInPruningRange = lastOffsetBeforeOrAtPruneUptoInclusive
        .filter(domainOffset => Option(domainOffset.offset) > pruneFromExclusive)
        .map(domainOffset =>
          (
            domainOffset.offset,
            CantonTimestamp(domainOffset.publicationTime),
          )
        )
      domainOffsets <- syncDomainPersistentStateManager.getAll.toList.parTraverseFilter {
        case (domainId, state) =>
          participantNodePersistentState.value.ledgerApiStore
            .lastDomainOffsetBeforeOrAt(domainId, pruneUpToInclusive)
            .flatMap(
              _.filter(domainOffset => Option(domainOffset.offset) > pruneFromExclusive)
                .map(domainOffset =>
                  state.requestJournalStore
                    .lastRequestCounterWithRequestTimestampBeforeOrAt(
                      CantonTimestamp(domainOffset.recordTime)
                    )
                    .map(requestCounterO =>
                      Some(
                        PruningCutoffs.DomainOffset(
                          state = state,
                          lastTimestamp = CantonTimestamp(domainOffset.recordTime),
                          lastRequestCounter = requestCounterO,
                        )
                      )
                    )
                )
                .getOrElse(FutureUnlessShutdown.pure(None))
            )
      }
    } yield PruningCutoffs(
      lastOffsetInPruningRange,
      domainOffsets,
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

    val domains = syncDomainPersistentStateManager.getAll.toList
    for {
      firstUnsafeOffsetO <- firstUnsafeOffset(domains, offset)
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
                unsafe.domainId,
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
      cutoffs <- lookUpDomainAndParticipantPruningCutoffs(fromExclusive, upToInclusive)

      archivedContracts <- lookUpContractsArchivedBeforeOrAt(fromExclusive, upToInclusive)

      // We must prune the contract store even if the event log is empty, because there is not necessarily an
      // archival event reassigned-away contracts.
      _ = logger.debug("Pruning contract store...")
      _ <- participantNodePersistentState.value.contractStore.deleteIgnoringUnknown(
        archivedContracts
      )

      _ <- cutoffs.domainOffsets.parTraverse(pruneDomain)
      _ <- cutoffs.globalOffsetO.fold(FutureUnlessShutdown.unit) {
        case (globalOffset, publicationTime) =>
          pruneDeduplicationStore(globalOffset, publicationTime)
      }
    } yield ()

  /** Prune a domain persistent state.
    *
    * @param archived  Contracts which have (by some external logic) been deemed safe to delete
    */
  private def pruneDomain(domainOffset: PruningCutoffs.DomainOffset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val PruningCutoffs.DomainOffset(state, lastTimestamp, lastRequestCounter) = domainOffset

    logger.info(
      show"Pruning ${state.indexedDomain.domainId} up to $lastTimestamp and request counter $lastRequestCounter"
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

  private def purgeDomain(state: SyncDomainPersistentState)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.info(s"Purging domain ${state.indexedDomain.domainId}")

    logger.debug("Purging active contract store...")
    for {
      // Purge stores that are pruned by the SyncDomain's JournalGarbageCollector as the SyncDomain
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
      logger.info(s"Purging domain ${state.indexedDomain.domainId} has been completed")
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
      domains: Seq[DomainId],
      participants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]] =
    for {
      allNoWait <- participantNodePersistentState.value.acsCounterParticipantConfigStore
        .getAllActiveNoWaitCounterParticipants(
          domains,
          participants,
        )
    } yield allNoWait

  def acsResetNoWaitCommitmentsFrom(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .removeNoWaitCounterParticipant(configs.map(_.domainId), configs.map(_.participantId))

  /** Providing the next Offset for iterative pruning: computed by the current pruning Offset increased by the max pruning batch size.
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
      domainId: DomainId,
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] =
    for {
      // This logic progressively lowers the timestamp based on the following constraints:
      // 1. Pruning must not delete data needed for recovery (after the clean replay timestamp)
      cleanReplayTs <- cleanReplayF

      // 2. Pruning must not delete events from the event log for which there are still in-flight submissions.
      // We check here the domain related events only.
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
              loggingContext.debug(s"Tick before or at $ts yields $tick on domain $domainId")
              FutureUnlessShutdown.pure(tick)
            case None =>
              FutureUnlessShutdown.failed(
                new RuntimeException(
                  s"Unable to compute tick before or at `$ts` for domain $domainId"
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

        s"Getting safe to prune commitment tick with data $timestamps on domain $domainId"
      }

      // Sanity check that safe pruning timestamp has not "increased" (which would be a coding bug).
      _ = tsSafeToPruneUpTo.foreach(ts =>
        ErrorUtil.requireState(
          ts <= latestTickBeforeOrAt,
          s"limit $tsSafeToPruneUpTo after $latestTickBeforeOrAt on domain $domainId",
        )
      )
    } yield tsSafeToPruneUpTo

  /** The latest commitment tick before or at the given time at which it is safe to prune. */
  def latestSafeToPruneTick(
      requestJournalStore: RequestJournalStore,
      domainIndexO: Option[DomainIndex],
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      acsCommitmentStore: AcsCommitmentStore,
      inFlightSubmissionStore: InFlightSubmissionStore,
      domainId: DomainId,
      checkForOutstandingCommitments: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val cleanReplayF = SyncDomainEphemeralStateFactory
      .crashRecoveryPruningBoundInclusive(requestJournalStore, domainIndexO)

    val commitmentsPruningBound =
      if (checkForOutstandingCommitments)
        CommitmentsPruningBound.Outstanding(ts => acsCommitmentStore.noOutstandingCommitments(ts))
      else
        CommitmentsPruningBound.LastComputedAndSent(
          acsCommitmentStore.lastComputedAndSent.map(_.map(_.forgetRefinement))
        )

    val earliestInFlightF = inFlightSubmissionStore.lookupEarliest(domainId)

    safeToPrune_(
      cleanReplayF,
      commitmentsPruningBound = commitmentsPruningBound,
      earliestInFlightF,
      sortedReconciliationIntervalsProvider,
      domainId,
    )
  }
  private final case class UnsafeOffset(
      offset: Offset,
      domainId: DomainId,
      recordTime: CantonTimestamp,
      cause: String,
  )

  /** PruningCutoffs captures two "formats" of the same pruning cutoff: The global offset and per-domain local offsets (with participant offset).
    * @param domainOffsets cutoff as domain-local offsets used for canton-internal per-domain pruning
    */
  final case class PruningCutoffs(
      globalOffsetO: Option[(Offset, CantonTimestamp)],
      domainOffsets: List[PruningCutoffs.DomainOffset],
  )

  object PruningCutoffs {

    /** @param state SyncDomainPersistentState of the domain
      * @param lastTimestamp Last sequencing timestamp below the given globalOffset
      * @param lastRequestCounter Last request counter below the given globalOffset
      */
    final case class DomainOffset(
        state: SyncDomainPersistentState,
        lastTimestamp: CantonTimestamp,
        lastRequestCounter: Option[RequestCounter],
    )
  }
}
