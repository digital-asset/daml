// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse_
import cats.instances.order.*
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData.ReassignmentGlobalOffset
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
  UnassignmentData,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, MapsUtil}
import monocle.Monocle.toAppliedFocusOps

import java.util.ConcurrentModificationException
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryReassignmentStore(
    synchronizer: Target[SynchronizerId],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ReassignmentStore
    with NamedLogging {

  import ReassignmentStore.*

  private[this] val reassignmentEntryMap: TrieMap[ReassignmentId, ReassignmentEntry] =
    new TrieMap[ReassignmentId, ReassignmentEntry]

  override def addUnassignmentData(
      unassignmentData: UnassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    ErrorUtil.requireArgument(
      unassignmentData.targetSynchronizer == synchronizer,
      s"Synchronizer $synchronizer: Reassignment store cannot store reassignment for synchronizer ${unassignmentData.targetSynchronizer}",
    )

    logger.debug(s"Add reassignment request in the store: ${unassignmentData.reassignmentId}")

    val reassignmentId = unassignmentData.reassignmentId
    val newEntry = ReassignmentEntry(unassignmentData, None, None)

    val result: Either[ReassignmentStoreError, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        ReassignmentStoreError,
        ReassignmentAlreadyCompleted,
        *,
      ], ReassignmentId, ReassignmentEntry](
        reassignmentEntryMap,
        reassignmentId,
        Checked.result(newEntry),
        merge(_, unassignmentData),
      )
      .toEither

    EitherT(FutureUnlessShutdown.pure(result))
  }

  private def merge(
      reassignmentEntry: ReassignmentEntry,
      otherUnassignmentData: UnassignmentData,
  ): Checked[ReassignmentStoreError, ReassignmentAlreadyCompleted, ReassignmentEntry] =
    for {
      reassignmentData <- reassignmentEntry.unassignmentDataO match {
        case None => Checked.result(otherUnassignmentData)
        case Some(oldUnassignmentData) => Checked.result(oldUnassignmentData)
      }
    } yield ReassignmentEntry(
      reassignmentData,
      reassignmentEntry.reassignmentGlobalOffset,
      reassignmentEntry.assignmentTs,
    )

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
          reassignmentEntryMap,
          reassignmentId,
          Left(UnknownReassignmentId(reassignmentId)),
          updateEntry,
        )

    EitherT(FutureUnlessShutdown.pure(res))
  }

  override def addReassignmentsOffsets(
      offsets: Map[ReassignmentId, reassignment.UnassignmentData.ReassignmentGlobalOffset]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = offsets.toList
    .parTraverse_ { case (reassignmentId, newGlobalOffset) =>
      editReassignmentEntry(
        reassignmentId,
        entry => addUnassignmentGlobalOffset(entry, newGlobalOffset),
      )
    }

  override def completeReassignment(reassignmentId: ReassignmentId, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = {
    logger.debug(s"Marking reassignment $reassignmentId as completed at $ts")

    CheckedT(FutureUnlessShutdown.pure {
      val result = MapsUtil
        .updateWithConcurrentlyChecked_[
          ReassignmentStoreError,
          ReassignmentAlreadyCompleted,
          ReassignmentId,
          ReassignmentEntry,
        ](
          reassignmentEntryMap,
          reassignmentId,
          Checked.abort(UnknownReassignmentId(reassignmentId)),
          entry => complete(entry, reassignmentId, ts),
        )
      result.toResult(())
    })
  }

  private def complete(
      reassignmentEntry: ReassignmentEntry,
      reassignmentId: ReassignmentId,
      ts: CantonTimestamp,
  ): Checked[ReassignmentStoreError, ReassignmentAlreadyCompleted, ReassignmentEntry] =
    reassignmentEntry.assignmentTs match {
      case Some(thisTs) if thisTs != ts =>
        Checked.continueWithResult(
          ReassignmentAlreadyCompleted(reassignmentId, ts),
          reassignmentEntry,
        )
      case _ =>
        Checked.result(reassignmentEntry.focus(_.assignmentTs).replace(Some(ts)))
    }

  private def addUnassignmentGlobalOffset(
      entry: ReassignmentEntry,
      offset: ReassignmentGlobalOffset,
  ): Either[ReassignmentGlobalOffsetsMerge, ReassignmentEntry] = {
    val newGlobalOffsetE = entry.reassignmentGlobalOffset
      .fold[Either[ReassignmentGlobalOffsetsMerge, ReassignmentGlobalOffset]](Right(offset))(
        _.merge(offset).leftMap(
          ReassignmentGlobalOffsetsMerge(entry.reassignmentId, _)
        )
      )

    newGlobalOffsetE.map(newGlobalOffset =>
      entry.copy(reassignmentGlobalOffset = Some(newGlobalOffset))
    )
  }

  override def deleteCompletionsSince(
      criterionInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    reassignmentEntryMap.foreach { case (reassignmentId, entry) =>
      val entryTsCompletion = entry.assignmentTs

      // Standard retry loop for clearing the completion in case the entry is updated concurrently.
      // Ensures progress as one writer succeeds in each iteration.
      @tailrec def clearCompletionCAS(): Unit = reassignmentEntryMap.get(reassignmentId) match {
        case None =>
          () // The reassignment ID has been deleted in the meantime so there's no completion to be cleared any more.
        case Some(newEntry) =>
          val newTsCompletion = newEntry.assignmentTs
          if (newTsCompletion.isDefined) {
            if (newTsCompletion != entryTsCompletion)
              throw new ConcurrentModificationException(
                s"Completion of reassignment $reassignmentId modified from $entryTsCompletion to $newTsCompletion while deleting completions."
              )
            val replaced =
              reassignmentEntryMap.replace(reassignmentId, newEntry, newEntry.clearCompletion)
            if (!replaced) clearCompletionCAS()
          }
      }

      /* First use the entry from the read-only snapshot the iteration goes over
       * If the entry's completion must be cleared in the snapshot but the entry has meanwhile been modified in the table
       * (e.g., an unassignment result has been added), then clear the table's entry.
       */
      if (entry.assignmentTs.exists(_ >= criterionInclusive)) {
        val replaced = reassignmentEntryMap.replace(reassignmentId, entry, entry.clearCompletion)
        if (!replaced) clearCompletionCAS()
      }
    }
  }

  override def lookup(
      reassignmentId: ReassignmentId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, UnassignmentData] =
    EitherT(FutureUnlessShutdown.pure {
      for {
        entry <- reassignmentEntryMap
          .get(reassignmentId)
          .toRight(UnknownReassignmentId(reassignmentId))
        _ <- entry.assignmentTs.map(ReassignmentCompleted(reassignmentId, _)).toLeft(())
        data <- entry.unassignmentDataO.toRight(
          AssignmentStartingBeforeUnassignment(reassignmentId)
        )
      } yield data
    })

  override def deleteReassignment(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    reassignmentEntryMap.remove(reassignmentId).discard
    FutureUnlessShutdown.unit
  }

  override def addAssignmentDataIfAbsent(assignmentData: AssignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    val newEntry = ReassignmentEntry(
      assignmentData.reassignmentId,
      assignmentData.contracts.contracts.map(_.contract),
      None,
      None,
      None,
    )

    val result: Either[ReassignmentStoreError, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        ReassignmentStoreError,
        ReassignmentAlreadyCompleted,
        *,
      ], ReassignmentId, ReassignmentEntry](
        reassignmentEntryMap,
        assignmentData.reassignmentId,
        Checked.result(newEntry),
        existing => Checked.result(existing),
      )
      .toEither

    EitherT(FutureUnlessShutdown.pure(result))
  }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[UnassignmentData]] = FutureUnlessShutdown.pure {
    def filter(entry: ReassignmentEntry): Boolean =
      entry.assignmentTs.isEmpty && // Always filter out completed assignment
        requestAfter.forall { case (ts, sourceSynchronizerID) =>
          (
            entry.unassignmentTs,
            entry.unassignmentDataO.map(_.sourceSynchronizer),
          ) > (ts, Some(sourceSynchronizerID))
        }

    reassignmentEntryMap.values
      .to(LazyList)
      .filter(filter)
      .flatMap(_.unassignmentDataO)
      .sortBy(t => (t.reassignmentId.unassignmentTs, t.reassignmentId.sourceSynchronizer.unwrap))(
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
      sourceSynchronizer: Option[Source[SynchronizerId]],
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]] = {
    def onlyUnassignmentCompleted(entry: ReassignmentEntry): Boolean =
      entry.unassignmentGlobalOffset.exists(_ <= validAt) &&
        entry.assignmentGlobalOffset.forall(_ > validAt)

    def onlyAssignmentCompleted(entry: ReassignmentEntry): Boolean =
      entry.assignmentGlobalOffset.exists(_ <= validAt) &&
        entry.unassignmentGlobalOffset.forall(_ > validAt)

    def incompleteReassignment(entry: ReassignmentEntry): Boolean =
      onlyUnassignmentCompleted(entry) || onlyAssignmentCompleted(entry)

    def filter(entry: ReassignmentEntry): Boolean =
      sourceSynchronizer.forall(_ == entry.sourceSynchronizer) &&
        incompleteReassignment(entry) && {
          val entryStakeholders = entry.contracts.forgetNE.flatMap(_.metadata.stakeholders)
          stakeholders.forall(_.exists(entryStakeholders.contains(_)))
        }

    val values = reassignmentEntryMap.values
      .to(LazyList)
      .collect {
        case entry if filter(entry) =>
          IncompleteReassignmentData
            .tryCreate(
              entry.sourceSynchronizer,
              entry.unassignmentTs,
              entry.unassignmentRequest,
              entry.reassignmentGlobalOffset,
              validAt,
            )
      }
      .sortBy(_.queryOffset)
      .take(limit.unwrap)

    FutureUnlessShutdown.pure(values)
  }

  override def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]] =
    // empty table: there are no reassignments
    if (reassignmentEntryMap.isEmpty) FutureUnlessShutdown.pure(None)
    else {
      def incompleteTransfer(entry: ReassignmentEntry): Boolean =
        (entry.assignmentGlobalOffset.isEmpty ||
          entry.unassignmentGlobalOffset.isEmpty)

      val incompleteReassignments = reassignmentEntryMap.values
        .to(LazyList)
        .filter(entry => incompleteTransfer(entry))
      // all reassignments are complete
      if (incompleteReassignments.isEmpty) FutureUnlessShutdown.pure(None)
      else {
        val incompleteReassignmentOffsets = incompleteReassignments
          .mapFilter(entry =>
            (
              entry.assignmentGlobalOffset
                .orElse(entry.unassignmentGlobalOffset),
              entry.reassignmentId,
            ) match {
              case (Some(o), rid) => Some((o, rid))
              case (None, _) => None
            }
          )
        // only in-flight reassignments
        if (incompleteReassignmentOffsets.isEmpty) FutureUnlessShutdown.pure(None)
        else {
          val default = (
            Offset.MaxValue,
            ReassignmentId(Source(synchronizer.unwrap), CantonTimestamp.MaxValue),
          )
          val minIncompleteTransferOffset =
            incompleteReassignmentOffsets.minByOption(_._1).getOrElse(default)
          FutureUnlessShutdown.pure(
            Some((minIncompleteTransferOffset._1, minIncompleteTransferOffset._2, synchronizer))
          )
        }
      }
    }

  override def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceSynchronizer: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]] =
    FutureUnlessShutdown.outcomeF(
      Future.successful(
        reassignmentEntryMap
          .filter { case (_reassignmentId, entry) =>
            entry.unassignmentRequest
              .map(_.contracts.contractIds)
              .exists(contractIds.contains) &&
            sourceSynchronizer.forall(source =>
              entry.unassignmentDataO.exists(_.sourceSynchronizer == source)
            ) &&
            unassignmentTs.forall(
              _ == entry.unassignmentTs
            ) &&
            completionTs.forall(ts => entry.assignmentTs.forall(ts == _))
          }
          .collect { case (reassignmentId, ReassignmentEntry(_, _, Some(request), _, _)) =>
            request.contracts.contractIds.map(_ -> reassignmentId)
          }
          .flatten
          .groupBy(_._1)
          .map { case (cid, values) => (cid, values.map(_._2).toSeq) }
      )
    )

  override def findReassignmentEntry(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownReassignmentId, ReassignmentEntry] =
    EitherT(
      FutureUnlessShutdown.pure(
        reassignmentEntryMap.get(reassignmentId).toRight(UnknownReassignmentId(reassignmentId))
      )
    )

  override def listInFlightReassignmentIds()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ReassignmentId]] =
    FutureUnlessShutdown.pure(
      reassignmentEntryMap.filter { case (_, entry) => entry.assignmentTs.isEmpty }.keys.toSeq
    )

}
