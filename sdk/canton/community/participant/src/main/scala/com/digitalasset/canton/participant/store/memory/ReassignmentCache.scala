// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  UnassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  ReassignmentAlreadyCompleted,
  ReassignmentCompleted,
  ReassignmentStoreError,
}
import com.digitalasset.canton.participant.store.memory.ReassignmentCache.PendingReassignmentCompletion
import com.digitalasset.canton.participant.store.{ReassignmentLookup, ReassignmentStore}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Adds an in-memory cache of pending completions on top of a [[store.ReassignmentStore]].
  * Completions appear atomic to reassignment lookups that go through the cache,
  * even if they are written to the store only later.
  */
class ReassignmentCache(
    reassignmentStore: ReassignmentStore,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends ReassignmentLookup
    with NamedLogging
    with FlagCloseable {

  @VisibleForTesting
  private[memory] val pendingCompletions
      : concurrent.Map[ReassignmentId, PendingReassignmentCompletion] =
    new TrieMap[ReassignmentId, PendingReassignmentCompletion]

  /** Completes the given reassignment with the given `tsCompletion`.
    * Completion appears atomic to reassignment lookups that go through the cache.
    *
    * @return The future completes when this completion or a completion of the same reassignment by an earlier request
    *         has been written to the underlying [[store.ReassignmentStore]].
    */
  def completeReassignment(reassignmentId: ReassignmentId, tsCompletion: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = CheckedT {
    logger.trace(
      s"Request at $tsCompletion: Marking reassignment $reassignmentId as completed in cache"
    )
    pendingCompletions.putIfAbsent(
      reassignmentId,
      PendingReassignmentCompletion(tsCompletion, futureSupervisor),
    ) match {
      case None =>
        reassignmentStore.completeReassignment(reassignmentId, tsCompletion).value.map { result =>
          val pendingReassignmentCompletion = pendingCompletions
            .remove(reassignmentId)
            .getOrElse(
              throw new IllegalStateException(
                s"Unable to find reassignment `$reassignmentId` in pending completions"
              )
            )
          pendingReassignmentCompletion.completion.success(UnlessShutdown.Outcome(result))
          result
        }

      case Some(
            pendingReassignmentCompletion @ PendingReassignmentCompletion(previousTs)
          ) =>
        if (previousTs <= tsCompletion) {
          /* An earlier request (or the same) is already writing to the reassignment store.
           * Therefore, there is no point in trying to store this later request, too.
           * It suffices to piggy-back on the earlier write and forward the result.
           */
          logger.debug(
            s"Request at $tsCompletion: Omitting the reassignment completion write because the earlier request at $previousTs is writing already."
          )
          pendingReassignmentCompletion.completion.futureUS.map { result =>
            for {
              _ <- result
              _ <-
                if (previousTs == tsCompletion) Checked.result(())
                else
                  Checked.continue(ReassignmentAlreadyCompleted(reassignmentId, tsCompletion))
            } yield ()
          }

        } else {
          /* A later request is already writing to the reassignment store.
           * To ensure that the earliest assignment request is recorded, we write the request to the store.
           * This write happens only after the ongoing write to not disturb the error reporting for the ongoing write.
           * However, it is not necessary to add this request to the cache
           * because the cache has already marked the reassignment as having been completed.
           */
          for {
            _ <- pendingReassignmentCompletion.completion.futureUS
            _ = logger.debug(
              s"Request at $tsCompletion: Overwriting the reassignment completion of the later request at $previousTs"
            )
            result <- reassignmentStore.completeReassignment(reassignmentId, tsCompletion).value
          } yield result
        }
    }
  }

  override def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStore.ReassignmentLookupError, UnassignmentData] =
    pendingCompletions.get(reassignmentId).fold(reassignmentStore.lookup(reassignmentId)) {
      case PendingReassignmentCompletion(tsCompletion) =>
        EitherT.leftT(ReassignmentCompleted(reassignmentId, tsCompletion))
    }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[UnassignmentData]] = reassignmentStore
    .findAfter(requestAfter, limit)
    .map(
      _.filter(reassignmentData => !pendingCompletions.contains(reassignmentData.reassignmentId))
    )

  /** unassignment/assignment global offsets will be updated upon publication on Ledger API Indexer, when
    * the global offset is assigned to the event.
    * In order to avoid race conditions, the multi-synchronizer event log will wait for the calls to
    * `ReassignmentStore.addReassignmentOffsets` to complete before updating ledger end.
    * Hence, we don't need additional synchronization here and we can directly query the store.
    */
  override def findIncomplete(
      sourceSynchronizer: Option[Source[SynchronizerId]],
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]] =
    reassignmentStore.findIncomplete(sourceSynchronizer, validAt, stakeholders, limit)

  def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]] =
    reassignmentStore.findEarliestIncomplete()

  override def findReassignmentEntry(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    ReassignmentStore.UnknownReassignmentId,
    ReassignmentStore.ReassignmentEntry,
  ] =
    reassignmentStore.findReassignmentEntry(reassignmentId)

  override def onClosed(): Unit =
    pendingCompletions.foreach { case (_, promise) =>
      promise.completion.shutdown()
    }

  override def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceSynchronizer: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]] =
    reassignmentStore.findContractReassignmentId(
      contractIds,
      sourceSynchronizer,
      unassignmentTs,
      completionTs,
    )
}

object ReassignmentCache {
  final case class PendingReassignmentCompletion(tsCompletion: CantonTimestamp)(
      val completion: PromiseUnlessShutdown[Checked[Nothing, ReassignmentStoreError, Unit]]
  )

  object PendingReassignmentCompletion {
    def apply(ts: CantonTimestamp, futureSupervisor: FutureSupervisor)(implicit
        ecl: ErrorLoggingContext
    ): PendingReassignmentCompletion = {

      val promise =
        PromiseUnlessShutdown.supervised[Checked[Nothing, ReassignmentStoreError, Unit]](
          s"pending completion of reassignment with ts=$ts",
          futureSupervisor,
        )

      PendingReassignmentCompletion(ts)(promise)
    }
  }
}
