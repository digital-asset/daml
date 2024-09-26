// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverseFilter.*
import cats.{Eval, Monad}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.Pruning.{
  LedgerPruningCancelledDueToShutdown,
  LedgerPruningError,
  LedgerPruningInternalError,
  LedgerPruningNotPossibleDuringHardMigration,
  LedgerPruningNothingToPrune,
}
import com.digitalasset.canton.participant.metrics.PruningMetrics
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  ParticipantNodePersistentState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.{GlobalOffset, Pruning}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil, SimpleExecutionQueue}
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

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
    FutureUtil.doNotAwait(
      executionQueue
        .execute(
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
        )
        .onShutdown(logger.debug("Pruning aborted due to shutdown")),
      "Unable to retrieve pruning status.",
      level = if (isClosing) Level.INFO else Level.ERROR,
    )

  /** Prune ledger event stream of this participant up to the given global offset inclusively.
    * Returns the global offset of the last pruned event.
    *
    * Safe to call multiple times concurrently.
    */
  def pruneLedgerEvents(
      pruneUpToInclusive: GlobalOffset
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = {

    def go(lastUpTo: Option[GlobalOffset]): Future[
      Either[Option[GlobalOffset], Either[LedgerPruningError, Unit]]
    ] = {
      val pruneUpToNext = increaseByBatchSize(lastUpTo)
      val offset = pruneUpToNext.min(pruneUpToInclusive)
      val done = offset == pruneUpToInclusive
      pruneLedgerEventBatch(lastUpTo, offset).transform {
        case Left(e) => Right(Left(e))
        case Right(_) if done => Right(Right(()))
        case Right(_) => Left(Some(offset))
      }.value
    }

    def doPrune(): EitherT[Future, LedgerPruningError, Unit] =
      EitherTUtil.timed(metrics.overall)(
        for {
          pruningStatus <- EitherT.right(
            participantNodePersistentState.value.pruningStore.pruningStatus()
          )
          _ensuredSafeToPrune <- ensurePruningOffsetIsSafe(pruneUpToInclusive)
          _prunedAllEventBatches <- EitherT(Monad[Future].tailRecM(pruningStatus.completedO)(go))
        } yield ()
      )
    executionQueue.executeE(doPrune(), s"prune ledger events upto $pruneUpToInclusive")
  }

  /** Returns an offset of at most `boundInclusive` that is safe to prune and whose timestamp is before or at `beforeOrAt`.
    *
    * @param boundInclusive The caller must choose a bound so that the ledger API server never requests an offset at or below `boundInclusive`.
    *                       Offsets at or below ledger end are typically a safe choice.
    */
  def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[GlobalOffset]] = EitherT(
    participantNodePersistentState.value.ledgerApiStore
      .lastDomainOffsetBeforeOrAtPublicationTime(beforeOrAt)
      .map(_.map(_.offset))
      .flatMap {
        case Some(beforeOrAtOffset) =>
          // under the hood this computation not only pushes back the boundInclusive bound according to the beforeOrAt publication timestamp, but also pushes it back before the ledger-end
          val rewoundBoundInclusive: GlobalOffset =
            if (beforeOrAtOffset >= boundInclusive.toLedgerOffset) boundInclusive
            else GlobalOffset.tryFromLedgerOffset(beforeOrAtOffset)
          firstUnsafeOffset(
            syncDomainPersistentStateManager.getAll.toList,
            rewoundBoundInclusive,
          ).map(
            _.map(_.globalOffset)
              .flatMap(firstOffsetBefore)
              .filter(_ < rewoundBoundInclusive)
              .orElse(Some(rewoundBoundInclusive))
          ).value

        case None =>
          Future.successful(
            Left(LedgerPruningNothingToPrune)
          ) // nothing to prune, beforeOrAt is too low
      }
  )

  private def firstOffsetBefore(globalOffset: GlobalOffset): Option[GlobalOffset] =
    PositiveLong
      .create(globalOffset.toLong - 1)
      .fold(
        _ => None, // if nothing is before
        longOffset => Some(GlobalOffset.tryFromLong(longOffset.value)),
      )

  private def firstUnsafeOffset(
      allDomains: List[(DomainId, SyncDomainPersistentState)],
      pruneUptoInclusive: GlobalOffset,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[UnsafeOffset]] = {

    def firstUnsafeEventFor(
        domainId: DomainId,
        persistent: SyncDomainPersistentState,
    ): EitherT[Future, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        domainIndex <- EitherT.right(
          participantNodePersistentState.value.ledgerApiStore
            .domainIndex(domainId)
        )
        sortedReconciliationIntervalsProvider <- sortedReconciliationIntervalsProviderFactory
          .get(
            domainId,
            domainIndex.sequencerIndex.map(_.timestamp).getOrElse(CantonTimestamp.MinValue),
          )
          .leftMap(LedgerPruningInternalError)

        safeCommitmentTick <- EitherT
          .fromOptionF[Future, LedgerPruningError, CantonTimestampSecond](
            AcsCommitmentProcessor.safeToPrune(
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

        firstUnsafeOffsetO <- EitherT.right(
          participantNodePersistentState.value.ledgerApiStore.firstDomainOffsetAfterOrAt(
            domainId,
            safeCommitmentTick.forgetRefinement,
          )
        )
      } yield {
        logger.debug(s"First unsafe pruning offset for domain $domainId at $firstUnsafeOffsetO")
        firstUnsafeOffsetO.map(domainOffset =>
          UnsafeOffset(
            globalOffset = GlobalOffset.tryFromLedgerOffset(domainOffset.offset),
            domainId = domainId,
            recordTime = CantonTimestamp(domainOffset.recordTime),
            cause = s"ACS background reconciliation and crash recovery",
          )
        )
      }

    def firstUnsafeReassignmentEventFor(
        domainId: DomainId,
        persistent: SyncDomainPersistentState,
    ): EitherT[Future, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        earliestIncompleteReassignmentO <- EitherT.right(
          persistent.reassignmentStore.findEarliestIncomplete()
        )

        unsafeOffset <- earliestIncompleteReassignmentO.fold(
          EitherT.rightT[Future, LedgerPruningError](None: Option[UnsafeOffset])
        ) { earliestIncompleteReassignment =>
          val (
            earliestIncompleteReassignmentGlobalOffset,
            earliestIncompleteReassignmentId,
            targetDomainId,
          ) = earliestIncompleteReassignment
          for {
            unsafeOffsetForReassignments <- EitherT(
              participantNodePersistentState.value.ledgerApiStore
                .domainOffset(earliestIncompleteReassignmentGlobalOffset.toLedgerOffset)
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
                GlobalOffset.tryFromLedgerOffset(unsafeOffsetForReassignments.offset),
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
    def firstUnsafeOffsetPublicationTime: Future[Option[UnsafeOffset]] = {
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
              participantNodePersistentState.value.ledgerApiStore.ledgerEndCache.publicationTime
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
              globalOffset = GlobalOffset.tryFromLedgerOffset(domainOffset.offset),
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
      _ <- EitherT.cond[Future](
        participantNodePersistentState.value.ledgerApiStore
          .ledgerEndCache()
          ._1
          .toLong >= pruneUptoInclusive.toLong,
        (),
        Pruning.LedgerPruningOffsetAfterLedgerEnd: LedgerPruningError,
      )
      allActiveDomains <- EitherT.fromEither[Future](allActiveDomainsE)
      affectedDomainsOffsets <- EitherT.right[LedgerPruningError](allActiveDomains.parFilterA {
        case (domainId, _persistent) =>
          participantNodePersistentState.value.ledgerApiStore
            .lastDomainOffsetBeforeOrAt(domainId, pruneUptoInclusive.toLedgerOffset)
            .map(_.isDefined)
      })
      _ <- EitherT.cond[Future](
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
      unsafeDedupOffset <- EitherT.right(firstUnsafeOffsetPublicationTime)
    } yield (unsafeDedupOffset.toList ++ unsafeDomainOffsets ++ unsafeIncompleteReassignmentOffsets)
      .minByOption(_.globalOffset)
  }

  private def pruneLedgerEventBatch(
      lastUpTo: Option[GlobalOffset],
      pruneUpToInclusiveBatchEnd: GlobalOffset,
  )(implicit traceContext: TraceContext): EitherT[Future, LedgerPruningError, Unit] =
    performUnlessClosingEitherT[LedgerPruningError, Unit](
      functionFullName,
      LedgerPruningCancelledDueToShutdown,
    ) {
      logger.info(s"Start pruning up to $pruneUpToInclusiveBatchEnd...")
      val pruningStore = participantNodePersistentState.value.pruningStore
      for {
        _ <- EitherT.right(pruningStore.markPruningStarted(pruneUpToInclusiveBatchEnd))
        _ <- EitherT.right(performPruning(lastUpTo, pruneUpToInclusiveBatchEnd))
        _ <- EitherT.right(pruningStore.markPruningDone(pruneUpToInclusiveBatchEnd))
      } yield {
        logger.info(s"Pruned up to $pruneUpToInclusiveBatchEnd")
      }
    }

  private def lookUpDomainAndParticipantPruningCutoffs(
      pruneFromExclusive: Option[GlobalOffset],
      pruneUpToInclusive: GlobalOffset,
  )(implicit traceContext: TraceContext): Future[PruningCutoffs] =
    for {
      lastOffsetBeforeOrAtPruneUptoInclusive <- participantNodePersistentState.value.ledgerApiStore
        .lastDomainOffsetBeforeOrAt(pruneUpToInclusive.toLedgerOffset)
      lastOffsetInPruningRange = lastOffsetBeforeOrAtPruneUptoInclusive
        .filter(_.offset.toLong > pruneFromExclusive.map(_.toLong).getOrElse(0L))
        .map(domainOffset =>
          (
            GlobalOffset.tryFromLedgerOffset(domainOffset.offset),
            CantonTimestamp(domainOffset.publicationTime),
          )
        )
      domainOffsets <- syncDomainPersistentStateManager.getAll.toList.parTraverseFilter {
        case (domainId, state) =>
          participantNodePersistentState.value.ledgerApiStore
            .lastDomainOffsetBeforeOrAt(domainId, pruneUpToInclusive.toLedgerOffset)
            .flatMap(
              _.filter(_.offset.toLong > pruneFromExclusive.map(_.toLong).getOrElse(0L))
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
                .getOrElse(Future.successful(None))
            )
      }
    } yield PruningCutoffs(
      lastOffsetInPruningRange,
      domainOffsets,
    )

  private def lookUpContractsArchivedBeforeOrAt(
      fromExclusive: Option[GlobalOffset],
      upToInclusive: GlobalOffset,
  )(implicit traceContext: TraceContext): Future[Set[LfContractId]] =
    participantNodePersistentState.value.ledgerApiStore.archivals(
      fromExclusive.map(_.toLedgerOffset),
      upToInclusive.toLedgerOffset,
    )

  private def ensurePruningOffsetIsSafe(
      globalOffset: GlobalOffset
  )(implicit traceContext: TraceContext): EitherT[Future, LedgerPruningError, Unit] = {

    val domains = syncDomainPersistentStateManager.getAll.toList
    for {
      firstUnsafeOffsetO <- firstUnsafeOffset(domains, globalOffset)
        // if nothing to prune we go on with this iteration regardless to ensure that iterative and scheduled pruning is not stuck in a window where nothing to prune
        .recover { case LedgerPruningNothingToPrune => None }
      _ <- firstUnsafeOffsetO match {
        case None => EitherT.pure[Future, LedgerPruningError](())
        case Some(unsafe) if unsafe.globalOffset > globalOffset =>
          EitherT.pure[Future, LedgerPruningError](())
        case Some(unsafe) =>
          EitherT
            .leftT[Future, Unit]
            .apply[LedgerPruningError](
              Pruning.LedgerPruningOffsetUnsafeToPrune(
                globalOffset,
                unsafe.domainId,
                unsafe.recordTime,
                unsafe.cause,
                firstOffsetBefore(unsafe.globalOffset),
              )
            )
      }
    } yield ()
  }

  private[pruning] def performPruning(
      fromExclusive: Option[GlobalOffset],
      upToInclusive: GlobalOffset,
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      cutoffs <- lookUpDomainAndParticipantPruningCutoffs(fromExclusive, upToInclusive)
      archivedContracts <- lookUpContractsArchivedBeforeOrAt(fromExclusive, upToInclusive)
      _ <- cutoffs.domainOffsets.parTraverse(pruneDomain(archivedContracts))
      _ <- cutoffs.globalOffsetO.fold(Future.unit) { case (globalOffset, publicationTime) =>
        pruneDeduplicationStore(globalOffset, publicationTime)
      }
    } yield ()

  /** Prune a domain persistent state.
    *
    * @param archived  Contracts which have (by some external logic) been deemed safe to delete
    */
  private def pruneDomain(
      archived: Iterable[LfContractId]
  )(domainOffset: PruningCutoffs.DomainOffset)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val PruningCutoffs.DomainOffset(state, lastTimestamp, lastRequestCounter) = domainOffset

    logger.info(
      show"Pruning ${state.domainId.item} up to $lastTimestamp and request counter $lastRequestCounter"
    )
    logger.debug("Pruning contract store...")

    for {
      // We must prune the contract store even if the event log is empty, because there is not necessarily an archival event
      // for divulged contracts or reassigned-away contracts.
      _ <- state.contractStore.deleteIgnoringUnknown(archived)

      _ <- lastRequestCounter.fold(Future.unit)(state.contractStore.deleteDivulged)

      _ = logger.debug("Pruning sequenced event store...")
      // we don't prune stores that are pruned by the PruneObserver regularly anyway
      _ <- state.sequencedEventStore.prune(lastTimestamp)

      _ = logger.debug("Pruning request journal store...")
      _ <- state.requestJournalStore.prune(lastTimestamp)

      _ = logger.debug("Pruning acs commitment store...")
      _ <- state.acsCommitmentStore.prune(lastTimestamp)
      // TODO(#2600) Prune the reassignment store
    } yield ()
  }

  private def pruneDeduplicationStore(
      globalOffset: GlobalOffset,
      publicationTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(
      s"Pruning command deduplication table at $globalOffset with publication time $publicationTime..."
    )
    participantNodePersistentState.value.commandDeduplicationStore
      .prune(globalOffset, publicationTime)
  }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)

  /** Providing the next Offset for iterative pruning: computed by the current pruning Offset increased by the max pruning batch size.
    */
  def locatePruningOffsetForOneIteration(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, GlobalOffset] =
    EitherT
      .right(
        participantNodePersistentState.value.pruningStore.pruningStatus()
      )
      .map(_.completedO)
      .map(increaseByBatchSize)

  private def increaseByBatchSize(globalOffset: Option[GlobalOffset]): GlobalOffset =
    GlobalOffset.tryFromLong(globalOffset.map(_.toLong).getOrElse(0L) + maxPruningBatchSize.value)

}

private[pruning] object PruningProcessor {
  private final case class UnsafeOffset(
      globalOffset: GlobalOffset,
      domainId: DomainId,
      recordTime: CantonTimestamp,
      cause: String,
  )

  /** PruningCutoffs captures two "formats" of the same pruning cutoff: The global offset and per-domain local offsets (with participant offset).
    * @param domainOffsets cutoff as domain-local offsets used for canton-internal per-domain pruning
    */
  final case class PruningCutoffs(
      globalOffsetO: Option[(GlobalOffset, CantonTimestamp)],
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
