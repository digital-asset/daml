// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.{
  IncompleteReassignmentData,
  ReassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  ReassignmentAlreadyCompleted,
  ReassignmentCompleted,
  ReassignmentStoreError,
}
import com.digitalasset.canton.participant.store.memory.ReassignmentCache.PendingReassignmentCompletion
import com.digitalasset.canton.participant.store.{ReassignmentLookup, ReassignmentStore}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{ReassignmentId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Adds an in-memory cache of pending completions on top of a [[store.ReassignmentStore]].
  * Completions appear atomic to reassignment lookups that go through the cache,
  * even if they are written to the store only later.
  */
class ReassignmentCache(
    reassignmentStore: ReassignmentStore,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends ReassignmentLookup
    with NamedLogging {

  @VisibleForTesting
  private[memory] val pendingCompletions
      : concurrent.Map[ReassignmentId, PendingReassignmentCompletion] =
    new TrieMap[ReassignmentId, PendingReassignmentCompletion]

  /** Completes the given reassignment with the given `timeOfCompletion`.
    * Completion appears atomic to reassignment lookups that go through the cache.
    *
    * @return The future completes when this completion or a completion of the same reassignment by an earlier request
    *         has been written to the underlying [[store.ReassignmentStore]].
    */
  def completeReassignment(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, ReassignmentStoreError, Unit] = CheckedT {
    logger.trace(
      s"Request ${timeOfCompletion.rc}: Marking reassignment $reassignmentId as completed in cache"
    )
    pendingCompletions.putIfAbsent(
      reassignmentId,
      PendingReassignmentCompletion(timeOfCompletion)(),
    ) match {
      case None =>
        reassignmentStore.completeReasignment(reassignmentId, timeOfCompletion).value.map {
          result =>
            logger
              .trace(
                s"Request ${timeOfCompletion.rc}: Marked reassignment $reassignmentId as completed"
              )
            val pendingReassignmentCompletion = pendingCompletions
              .remove(reassignmentId)
              .getOrElse(
                throw new IllegalStateException(
                  s"Unable to find reassignment `$reassignmentId` in pending completions"
                )
              )
            pendingReassignmentCompletion.completion.success(result)
            result
        }

      case Some(
            pendingReassignmentCompletion @ PendingReassignmentCompletion(previousTimeOfCompletion)
          ) =>
        if (previousTimeOfCompletion.rc <= timeOfCompletion.rc) {
          /* An earlier request (or the same) is already writing to the reassignment store.
           * Therefore, there is no point in trying to store this later request, too.
           * It suffices to piggy-back on the earlier write and forward the result.
           */
          logger.trace(
            s"Request ${timeOfCompletion.rc}: Omitting the reassignment completion write because the earlier request ${previousTimeOfCompletion.rc} is writing already."
          )
          pendingReassignmentCompletion.completion.future.map { result =>
            for {
              _ <- result
              _ <-
                if (previousTimeOfCompletion == timeOfCompletion) Checked.result(())
                else
                  Checked.continue(ReassignmentAlreadyCompleted(reassignmentId, timeOfCompletion))
            } yield ()
          }
        } else {
          /* A later request is already writing to the reassignment store.
           * To ensure that the earliest assignment request is recorded, we write the request to the store.`
           * This write happens only after the ongoing write to not disturb the error reporting for the ongoing write.
           * However, it is not necessary to add this request to the cache
           * because the cache has already marked the reassignment as having been completed.
           */
          for {
            _ <- pendingReassignmentCompletion.completion.future
            _ = logger.trace(
              s"Request ${timeOfCompletion.rc}: Overwriting the reassignment completion of the later request ${previousTimeOfCompletion.rc}"
            )
            result <- reassignmentStore.completeReasignment(reassignmentId, timeOfCompletion).value
          } yield result
        }
    }
  }

  override def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentStore.ReassignmentLookupError, ReassignmentData] =
    pendingCompletions.get(reassignmentId).fold(reassignmentStore.lookup(reassignmentId)) {
      case PendingReassignmentCompletion(timeOfCompletion) =>
        EitherT.leftT(ReassignmentCompleted(reassignmentId, timeOfCompletion))
    }

  override def find(
      filterSource: Option[SourceDomainId],
      filterRequestTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[ReassignmentData]] =
    reassignmentStore
      .find(filterSource, filterRequestTimestamp, filterSubmitter, limit)
      .map(
        _.filter(reassignmentData => !pendingCompletions.contains(reassignmentData.reassignmentId))
      )

  override def findAfter(requestAfter: Option[(CantonTimestamp, SourceDomainId)], limit: Int)(
      implicit traceContext: TraceContext
  ): Future[Seq[ReassignmentData]] = reassignmentStore
    .findAfter(requestAfter, limit)
    .map(
      _.filter(reassignmentData => !pendingCompletions.contains(reassignmentData.reassignmentId))
    )

  /** unassignment/assignment global offsets will be updated upon publication in the multi-domain event log, when
    * the global offset is assigned to the event.
    * In order to avoid race conditions, the multi-domain event log will wait for the calls to
    * `ReassignmentStore.addReassignmentOffsets` to complete before updating ledger end.
    * Hence, we don't need additional synchronization here and we can directly query the store.
    */
  override def findIncomplete(
      sourceDomain: Option[SourceDomainId],
      validAt: GlobalOffset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): Future[Seq[IncompleteReassignmentData]] =
    reassignmentStore.findIncomplete(sourceDomain, validAt, stakeholders, limit)

  def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, ReassignmentId, TargetDomainId)]] =
    reassignmentStore.findEarliestIncomplete()
}

object ReassignmentCache {
  final case class PendingReassignmentCompletion(timeOfCompletion: TimeOfChange)(
      val completion: Promise[Checked[Nothing, ReassignmentStoreError, Unit]] =
        Promise[Checked[Nothing, ReassignmentStoreError, Unit]]()
  )
}
