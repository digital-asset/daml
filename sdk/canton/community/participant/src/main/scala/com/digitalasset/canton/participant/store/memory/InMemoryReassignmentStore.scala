// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.instances.order.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  ReassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, MapsUtil}
import com.digitalasset.canton.{LfPartyId, RequestCounter}

import java.util.ConcurrentModificationException
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryReassignmentStore(
    domain: Target[SynchronizerId],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ReassignmentStore
    with NamedLogging {

  import ReassignmentStore.*

  private[this] val reassignmentDataMap: TrieMap[ReassignmentId, ReassignmentEntry] =
    new TrieMap[ReassignmentId, ReassignmentEntry]

  override def addReassignment(
      reassignmentData: ReassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    ErrorUtil.requireArgument(
      reassignmentData.targetDomain == domain,
      s"Domain $domain: Reassignment store cannot store reassignment for domain ${reassignmentData.targetDomain}",
    )

    val reassignmentId = reassignmentData.reassignmentId
    val newEntry = ReassignmentEntry(reassignmentData, None)

    val result: Either[ReassignmentDataAlreadyExists, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        ReassignmentDataAlreadyExists,
        ReassignmentAlreadyCompleted,
        *,
      ], ReassignmentId, ReassignmentEntry](
        reassignmentDataMap,
        reassignmentId,
        Checked.result(newEntry),
        _.mergeWith(newEntry),
      )
      .toEither

    EitherT(FutureUnlessShutdown.pure(result))
  }

  private def editReassignmentEntry(
      reassignmentId: ReassignmentId,
      updateEntry: ReassignmentEntry => Either[ReassignmentStoreError, ReassignmentEntry],
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    val res =
      MapsUtil
        .updateWithConcurrentlyM_[Either[
          ReassignmentStoreError,
          *,
        ], ReassignmentId, ReassignmentEntry](
          reassignmentDataMap,
          reassignmentId,
          Left(UnknownReassignmentId(reassignmentId)),
          updateEntry,
        )

    EitherT(FutureUnlessShutdown.pure(res))
  }

  override def addUnassignmentResult(
      unassignmentResult: DeliveredUnassignmentResult
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    val reassignmentId = unassignmentResult.reassignmentId
    editReassignmentEntry(reassignmentId, _.addUnassignmentResult(unassignmentResult))
  }

  override def addReassignmentsOffsets(
      offsets: Map[ReassignmentId, reassignment.ReassignmentData.ReassignmentGlobalOffset]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = offsets.toList
    .parTraverse_ { case (reassignmentId, newGlobalOffset) =>
      editReassignmentEntry(reassignmentId, _.addUnassignmentGlobalOffset(newGlobalOffset))
    }

  override def completeReassignment(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] =
    CheckedT(FutureUnlessShutdown.pure {
      val result = MapsUtil
        .updateWithConcurrentlyChecked_[
          ReassignmentStoreError,
          ReassignmentAlreadyCompleted,
          ReassignmentId,
          ReassignmentEntry,
        ](
          reassignmentDataMap,
          reassignmentId,
          Checked.abort(UnknownReassignmentId(reassignmentId)),
          _.complete(timeOfCompletion),
        )
      result.toResult(())
    })

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful {
    reassignmentDataMap.foreach { case (reassignmentId, entry) =>
      val entryTimeOfCompletion = entry.timeOfCompletion

      // Standard retry loop for clearing the completion in case the entry is updated concurrently.
      // Ensures progress as one writer succeeds in each iteration.
      @tailrec def clearCompletionCAS(): Unit = reassignmentDataMap.get(reassignmentId) match {
        case None =>
          () // The reassignment ID has been deleted in the meantime so there's no completion to be cleared any more.
        case Some(newEntry) =>
          val newTimeOfCompletion = newEntry.timeOfCompletion
          if (newTimeOfCompletion.isDefined) {
            if (newTimeOfCompletion != entryTimeOfCompletion)
              throw new ConcurrentModificationException(
                s"Completion of reassignment $reassignmentId modified from $entryTimeOfCompletion to $newTimeOfCompletion while deleting completions."
              )
            val replaced =
              reassignmentDataMap.replace(reassignmentId, newEntry, newEntry.clearCompletion)
            if (!replaced) clearCompletionCAS()
          }
      }

      /* First use the entry from the read-only snapshot the iteration goes over
       * If the entry's completion must be cleared in the snapshot but the entry has meanwhile been modified in the table
       * (e.g., an unassignment result has been added), then clear the table's entry.
       */
      if (entry.timeOfCompletion.exists(_.rc >= criterionInclusive)) {
        val replaced = reassignmentDataMap.replace(reassignmentId, entry, entry.clearCompletion)
        if (!replaced) clearCompletionCAS()
      }
    }
  }

  override def lookup(
      reassignmentId: ReassignmentId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, ReassignmentData] =
    EitherT(FutureUnlessShutdown.pure {
      for {
        entry <- reassignmentDataMap
          .get(reassignmentId)
          .toRight(UnknownReassignmentId(reassignmentId))
        _ <- entry.timeOfCompletion.map(ReassignmentCompleted(reassignmentId, _)).toLeft(())
      } yield entry.reassignmentData
    })

  override def find(
      filterSource: Option[Source[SynchronizerId]],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]] =
    FutureUnlessShutdown.pure {
      def filter(entry: ReassignmentEntry): Boolean =
        entry.timeOfCompletion.isEmpty && // Always filter out completed assignment
          filterSource.forall(source => entry.reassignmentData.sourceDomain == source) &&
          filterTimestamp.forall(ts =>
            entry.reassignmentData.reassignmentId.unassignmentTs == ts
          ) &&
          filterSubmitter.forall(party =>
            entry.reassignmentData.unassignmentRequest.submitter == party
          )

      reassignmentDataMap.values.to(LazyList).filter(filter).take(limit).map(_.reassignmentData)
    }

  override def deleteReassignment(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    reassignmentDataMap.remove(reassignmentId).discard
    FutureUnlessShutdown.unit
  }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ReassignmentData]] = FutureUnlessShutdown.pure {
    def filter(entry: ReassignmentEntry): Boolean =
      entry.timeOfCompletion.isEmpty && // Always filter out completed assignment
        requestAfter.forall(ts =>
          (
            entry.reassignmentData.reassignmentId.unassignmentTs,
            entry.reassignmentData.sourceDomain,
          ) > ts
        )

    reassignmentDataMap.values
      .to(LazyList)
      .filter(filter)
      .map(_.reassignmentData)
      .sortBy(t => (t.reassignmentId.unassignmentTs, t.reassignmentId.sourceDomain.unwrap))(
        // Explicitly use the standard ordering on two-tuples here
        // As Scala does not seem to infer the right implicits to use here
        Ordering.Tuple2(
          CantonTimestamp.orderCantonTimestamp.toOrdering,
          SynchronizerId.orderSynchronizerId.toOrdering,
        )
      )
      .take(limit)
  }

  override def findIncomplete(
      sourceDomain: Option[Source[SynchronizerId]],
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]] = {
    def onlyUnassignmentCompleted(entry: ReassignmentEntry): Boolean =
      entry.reassignmentData.unassignmentGlobalOffset.exists(_ <= validAt) &&
        entry.reassignmentData.assignmentGlobalOffset.forall(_ > validAt)

    def onlyAssignmentCompleted(entry: ReassignmentEntry): Boolean =
      entry.reassignmentData.assignmentGlobalOffset.exists(_ <= validAt) &&
        entry.reassignmentData.unassignmentGlobalOffset.forall(_ > validAt)

    def incompleteReassignment(entry: ReassignmentEntry): Boolean =
      onlyUnassignmentCompleted(entry) || onlyAssignmentCompleted(entry)

    def filter(entry: ReassignmentEntry): Boolean =
      sourceDomain.forall(_ == entry.reassignmentData.sourceDomain) &&
        incompleteReassignment(entry) &&
        stakeholders.forall(_.exists(entry.reassignmentData.contract.metadata.stakeholders))

    val values = reassignmentDataMap.values
      .to(LazyList)
      .collect {
        case entry if filter(entry) =>
          IncompleteReassignmentData.tryCreate(entry.reassignmentData, validAt)
      }
      .sortBy(_.queryOffset)
      .take(limit.unwrap)

    FutureUnlessShutdown.pure(values)
  }

  override def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]] =
    // empty table: there are no reassignments
    if (reassignmentDataMap.isEmpty) FutureUnlessShutdown.pure(None)
    else {
      def incompleteTransfer(entry: ReassignmentEntry): Boolean =
        (entry.reassignmentData.assignmentGlobalOffset.isEmpty ||
          entry.reassignmentData.unassignmentGlobalOffset.isEmpty) &&
          entry.reassignmentData.targetDomain == domain
      val incompleteReassignments = reassignmentDataMap.values
        .to(LazyList)
        .filter(entry => incompleteTransfer(entry))
      // all reassignments are complete
      if (incompleteReassignments.isEmpty) FutureUnlessShutdown.pure(None)
      else {
        val incompleteReassignmentOffsets = incompleteReassignments
          .mapFilter(entry =>
            (
              entry.reassignmentData.assignmentGlobalOffset
                .orElse(entry.reassignmentData.unassignmentGlobalOffset),
              entry.reassignmentData.reassignmentId,
            ) match {
              case (Some(o), reassignmentId) => Some((o, reassignmentId))
              case (None, _) => None
            }
          )
        // only in-flight reassignments
        if (incompleteReassignmentOffsets.isEmpty) FutureUnlessShutdown.pure(None)
        else {
          val default = (
            Offset.MaxValue,
            ReassignmentId(Source(domain.unwrap), CantonTimestamp.MaxValue),
          )
          val minIncompleteTransferOffset =
            incompleteReassignmentOffsets.minByOption(_._1).getOrElse(default)
          FutureUnlessShutdown.pure(
            Some((minIncompleteTransferOffset._1, minIncompleteTransferOffset._2, domain))
          )
        }
      }
    }

  override def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceDomain: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]] =
    FutureUnlessShutdown.outcomeF(
      Future.successful(
        reassignmentDataMap
          .filter { case (_reassignmentId, transferData) =>
            contractIds.contains(transferData.reassignmentData.contract.contractId) &&
            sourceDomain.forall(_ == transferData.reassignmentData.sourceDomain) &&
            unassignmentTs.forall(
              _ == transferData.reassignmentData.reassignmentId.unassignmentTs
            ) &&
            completionTs.forall(ts =>
              transferData.timeOfCompletion.forall(toc => ts == toc.timestamp)
            )
          }
          .map { case (reassignmentId, entry) =>
            (entry.reassignmentData.contract.contractId, reassignmentId)
          }
          .groupBy(_._1)
          .map { case (cid, value) => (cid, value.values.toSeq) }
      )
    )
}
