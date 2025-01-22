// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  AssignmentData,
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

  override def addReassignment(
      reassignmentData: ReassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] = {
    ErrorUtil.requireArgument(
      reassignmentData.targetSynchronizer == synchronizer,
      s"Synchronizer $synchronizer: Reassignment store cannot store reassignment for synchronizer ${reassignmentData.targetSynchronizer}",
    )

    logger.debug(s"Add reassignment request in the store: ${reassignmentData.reassignmentId}")

    val reassignmentId = reassignmentData.reassignmentId
    val newEntry = ReassignmentEntry(reassignmentData, None)

    val result: Either[ReassignmentDataAlreadyExists, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        ReassignmentDataAlreadyExists,
        ReassignmentAlreadyCompleted,
        *,
      ], ReassignmentId, ReassignmentEntry](
        reassignmentEntryMap,
        reassignmentId,
        Checked.result(newEntry),
        _.mergeWith(reassignmentData),
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
          reassignmentEntryMap,
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

  override def completeReassignment(reassignmentId: ReassignmentId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit] = {
    logger.debug(s"Marking reassignment $reassignmentId as completed at $toc")

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
          entry => complete(entry, reassignmentId, toc),
        )
      result.toResult(())
    })
  }

  private def complete(
      reassignmentEntry: ReassignmentEntry,
      reassignmentId: ReassignmentId,
      timeOfChange: TimeOfChange,
  ): Checked[ReassignmentDataAlreadyExists, ReassignmentAlreadyCompleted, ReassignmentEntry] =
    reassignmentEntry.timeOfCompletion match {
      case Some(thisToc) if thisToc != timeOfChange =>
        Checked.continueWithResult(
          ReassignmentAlreadyCompleted(reassignmentId, timeOfChange),
          reassignmentEntry,
        )
      case _ =>
        Checked.result(reassignmentEntry.focus(_.timeOfCompletion).replace(Some(timeOfChange)))
    }

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    reassignmentEntryMap.foreach { case (reassignmentId, entry) =>
      val entryTimeOfCompletion = entry.timeOfCompletion

      // Standard retry loop for clearing the completion in case the entry is updated concurrently.
      // Ensures progress as one writer succeeds in each iteration.
      @tailrec def clearCompletionCAS(): Unit = reassignmentEntryMap.get(reassignmentId) match {
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
              reassignmentEntryMap.replace(reassignmentId, newEntry, newEntry.clearCompletion)
            if (!replaced) clearCompletionCAS()
          }
      }

      /* First use the entry from the read-only snapshot the iteration goes over
       * If the entry's completion must be cleared in the snapshot but the entry has meanwhile been modified in the table
       * (e.g., an unassignment result has been added), then clear the table's entry.
       */
      if (entry.timeOfCompletion.exists(_.rc >= criterionInclusive)) {
        val replaced = reassignmentEntryMap.replace(reassignmentId, entry, entry.clearCompletion)
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
        entry <- reassignmentEntryMap
          .get(reassignmentId)
          .toRight(UnknownReassignmentId(reassignmentId))
        _ <- entry.timeOfCompletion.map(ReassignmentCompleted(reassignmentId, _)).toLeft(())
        data <- entry.reassignmentDataO.toRight(
          AssignmentStartingBeforeUnassignment(reassignmentId)
        )
      } yield data
    })

  override def find(
      filterSource: Option[Source[SynchronizerId]],
      filterTimestamp: Option[CantonTimestamp],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]] =
    FutureUnlessShutdown.pure {
      def filter(entry: ReassignmentEntry): Boolean =
        entry.timeOfCompletion.isEmpty && // Always filter out completed assignment
          filterSource.forall(source =>
            entry.reassignmentDataO.exists(_.sourceSynchronizer == source)
          ) &&
          filterTimestamp.forall(ts => entry.unassignmentTs == ts)
      reassignmentEntryMap.values
        .to(LazyList)
        .filter(filter)
        .take(limit)
        .flatMap(_.reassignmentDataO)
    }

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
      assignmentData.sourceProtocolVersion,
      assignmentData.reassignmentId.unassignmentTs,
      RequestCounter.MaxValue,
      None,
      CantonTimestamp.Epoch,
      None,
      None,
      None,
    )

    val result: Either[ReassignmentDataAlreadyExists, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        ReassignmentDataAlreadyExists,
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
  ): FutureUnlessShutdown[Seq[ReassignmentData]] = FutureUnlessShutdown.pure {
    def filter(entry: ReassignmentEntry): Boolean =
      entry.timeOfCompletion.isEmpty && // Always filter out completed assignment
        requestAfter.forall { case (ts, sourceSynchronizerID) =>
          (
            entry.unassignmentTs,
            entry.reassignmentDataO.map(_.sourceSynchronizer),
          ) > (ts, Some(sourceSynchronizerID))
        }

    reassignmentEntryMap.values
      .to(LazyList)
      .filter(filter)
      .flatMap(_.reassignmentDataO)
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
    def onlyUnassignmentCompleted(entry: ReassignmentData): Boolean =
      entry.unassignmentGlobalOffset.exists(_ <= validAt) &&
        entry.assignmentGlobalOffset.forall(_ > validAt)

    def onlyAssignmentCompleted(entry: ReassignmentData): Boolean =
      entry.assignmentGlobalOffset.exists(_ <= validAt) &&
        entry.unassignmentGlobalOffset.forall(_ > validAt)

    def incompleteReassignment(entry: ReassignmentData): Boolean =
      onlyUnassignmentCompleted(entry) || onlyAssignmentCompleted(entry)

    def filter(entry: ReassignmentData): Boolean =
      sourceSynchronizer.forall(_ == entry.sourceSynchronizer) &&
        incompleteReassignment(entry) &&
        stakeholders.forall(_.exists(entry.contract.metadata.stakeholders))

    val values = reassignmentEntryMap.values
      .to(LazyList)
      .collect {
        case ReassignmentEntry(
              _sourceProtocolVersion,
              unassignmentTs,
              unassignmentRequestCounter,
              Some(unassignmentRequest),
              unassignmentDecisionTime,
              unassignmentResult,
              reassignmentGlobalOffset,
              None,
            )
            if filter(
              ReassignmentData(
                unassignmentTs,
                unassignmentRequestCounter,
                unassignmentRequest,
                unassignmentDecisionTime,
                unassignmentResult,
                reassignmentGlobalOffset,
              )
            ) =>
          IncompleteReassignmentData.tryCreate(
            ReassignmentData(
              unassignmentTs,
              unassignmentRequestCounter,
              unassignmentRequest,
              unassignmentDecisionTime,
              unassignmentResult,
              reassignmentGlobalOffset,
            ),
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
      def incompleteTransfer(entry: ReassignmentData): Boolean =
        (entry.assignmentGlobalOffset.isEmpty ||
          entry.unassignmentGlobalOffset.isEmpty) &&
          entry.targetSynchronizer == synchronizer

      val incompleteReassignments = reassignmentEntryMap.values
        .to(LazyList)
        .flatMap(_.reassignmentDataO)
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
              case (Some(o), reassignmentId) => Some((o, reassignmentId))
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
              .map(_.contract.contractId)
              .exists(contractIds.contains) &&
            sourceSynchronizer.forall(source =>
              entry.reassignmentDataO.exists(_.sourceSynchronizer == source)
            ) &&
            unassignmentTs.forall(
              _ == entry.unassignmentTs
            ) &&
            completionTs.forall(ts => entry.timeOfCompletion.forall(toc => ts == toc.timestamp))
          }
          .collect { case (reassignmentId, ReassignmentEntry(_, _, _, Some(request), _, _, _, _)) =>
            (request.contract.contractId, reassignmentId)
          }
          .groupBy(_._1)
          .map { case (cid, value) => (cid, value.values.toSeq) }
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

}
