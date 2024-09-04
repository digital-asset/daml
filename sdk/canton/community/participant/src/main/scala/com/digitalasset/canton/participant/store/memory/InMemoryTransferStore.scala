// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.implicits.catsSyntaxPartialOrder
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.{IncompleteTransferData, TransferData}
import com.digitalasset.canton.participant.store.TransferStore
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{ReassignmentId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, MapsUtil}
import com.digitalasset.canton.{LfPartyId, RequestCounter}

import java.util.ConcurrentModificationException
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryTransferStore(
    domain: TargetDomainId,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TransferStore
    with NamedLogging {

  import TransferStore.*

  private[this] val transferDataMap: TrieMap[ReassignmentId, TransferEntry] =
    new TrieMap[ReassignmentId, TransferEntry]

  override def addTransfer(
      transferData: TransferData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = {
    ErrorUtil.requireArgument(
      transferData.targetDomain == domain,
      s"Domain ${domain.unwrap.unwrap}: Transfer store cannot store transfer for domain ${transferData.targetDomain.unwrap.unwrap}",
    )

    val reassignmentId = transferData.reassignmentId
    val newEntry = TransferEntry(transferData, None)

    val result: Either[TransferDataAlreadyExists, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        TransferDataAlreadyExists,
        TransferAlreadyCompleted,
        *,
      ], ReassignmentId, TransferEntry](
        transferDataMap,
        reassignmentId,
        Checked.result(newEntry),
        _.mergeWith(newEntry),
      )
      .toEither

    EitherT(FutureUnlessShutdown.pure(result))
  }

  private def editTransferEntry(
      reassignmentId: ReassignmentId,
      updateEntry: TransferEntry => Either[TransferStoreError, TransferEntry],
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = {
    val res =
      MapsUtil
        .updateWithConcurrentlyM_[Either[TransferStoreError, *], ReassignmentId, TransferEntry](
          transferDataMap,
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
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = {
    val reassignmentId = unassignmentResult.reassignmentId
    editTransferEntry(reassignmentId, _.addUnassignmentResult(unassignmentResult))
  }

  override def addTransfersOffsets(offsets: Map[ReassignmentId, TransferData.TransferGlobalOffset])(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = offsets.toList
    .parTraverse_ { case (reassignmentId, newGlobalOffset) =>
      editTransferEntry(reassignmentId, _.addUnassignmentGlobalOffset(newGlobalOffset))
    }

  override def completeTransfer(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit] =
    CheckedT(Future.successful {
      val result = MapsUtil
        .updateWithConcurrentlyChecked_[
          TransferStoreError,
          TransferAlreadyCompleted,
          ReassignmentId,
          TransferEntry,
        ](
          transferDataMap,
          reassignmentId,
          Checked.abort(UnknownReassignmentId(reassignmentId)),
          _.complete(timeOfCompletion),
        )
      result.toResult(())
    })

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful {
    transferDataMap.foreach { case (reassignmentId, entry) =>
      val entryTimeOfCompletion = entry.timeOfCompletion

      // Standard retry loop for clearing the completion in case the entry is updated concurrently.
      // Ensures progress as one writer succeeds in each iteration.
      @tailrec def clearCompletionCAS(): Unit = transferDataMap.get(reassignmentId) match {
        case None =>
          () // The reassignment ID has been deleted in the meantime so there's no completion to be cleared any more.
        case Some(newEntry) =>
          val newTimeOfCompletion = newEntry.timeOfCompletion
          if (newTimeOfCompletion.isDefined) {
            if (newTimeOfCompletion != entryTimeOfCompletion)
              throw new ConcurrentModificationException(
                s"Completion of transfer $reassignmentId modified from $entryTimeOfCompletion to $newTimeOfCompletion while deleting completions."
              )
            val replaced =
              transferDataMap.replace(reassignmentId, newEntry, newEntry.clearCompletion)
            if (!replaced) clearCompletionCAS()
          }
      }

      /* First use the entry from the read-only snapshot the iteration goes over
       * If the entry's completion must be cleared in the snapshot but the entry has meanwhile been modified in the table
       * (e.g., an unassignment result has been added), then clear the table's entry.
       */
      if (entry.timeOfCompletion.exists(_.rc >= criterionInclusive)) {
        val replaced = transferDataMap.replace(reassignmentId, entry, entry.clearCompletion)
        if (!replaced) clearCompletionCAS()
      }
    }
  }

  override def lookup(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): EitherT[Future, TransferLookupError, TransferData] =
    EitherT(Future.successful {
      for {
        entry <- transferDataMap.get(reassignmentId).toRight(UnknownReassignmentId(reassignmentId))
        _ <- entry.timeOfCompletion.map(TransferCompleted(reassignmentId, _)).toLeft(())
      } yield entry.transferData
    })

  override def find(
      filterSource: Option[SourceDomainId],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
    Future.successful {
      def filter(entry: TransferEntry): Boolean =
        entry.timeOfCompletion.isEmpty && // Always filter out completed assignment
          filterSource.forall(source => entry.transferData.sourceDomain == source) &&
          filterTimestamp.forall(ts => entry.transferData.reassignmentId.unassignmentTs == ts) &&
          filterSubmitter.forall(party => entry.transferData.unassignmentRequest.submitter == party)

      transferDataMap.values.to(LazyList).filter(filter).take(limit).map(_.transferData)
    }

  override def deleteTransfer(
      reassignmentId: ReassignmentId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    transferDataMap.remove(reassignmentId).discard
    Future.unit
  }

  override def findAfter(requestAfter: Option[(CantonTimestamp, SourceDomainId)], limit: Int)(
      implicit traceContext: TraceContext
  ): Future[Seq[TransferData]] = Future.successful {
    def filter(entry: TransferEntry): Boolean =
      entry.timeOfCompletion.isEmpty && // Always filter out completed assignment
        requestAfter.forall(ts =>
          (
            entry.transferData.reassignmentId.unassignmentTs,
            entry.transferData.sourceDomain,
          ) > ts
        )

    transferDataMap.values
      .to(LazyList)
      .filter(filter)
      .map(_.transferData)
      .sortBy(t => (t.reassignmentId.unassignmentTs, t.reassignmentId.sourceDomain))(
        // Explicitly use the standard ordering on two-tuples here
        // As Scala does not seem to infer the right implicits to use here
        Ordering.Tuple2(
          CantonTimestamp.orderCantonTimestamp.toOrdering,
          SourceDomainId.orderSourceDomainId.toOrdering,
        )
      )
      .take(limit)
  }

  override def findIncomplete(
      sourceDomain: Option[SourceDomainId],
      validAt: GlobalOffset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): Future[Seq[IncompleteTransferData]] = {
    def onlyUnassignmentCompleted(entry: TransferEntry): Boolean =
      entry.transferData.unassignmentGlobalOffset.exists(_ <= validAt) &&
        entry.transferData.assignmentGlobalOffset.forall(_ > validAt)

    def onlyAssignmentCompleted(entry: TransferEntry): Boolean =
      entry.transferData.assignmentGlobalOffset.exists(_ <= validAt) &&
        entry.transferData.unassignmentGlobalOffset.forall(_ > validAt)

    def incompleteTransfer(entry: TransferEntry): Boolean =
      onlyUnassignmentCompleted(entry) || onlyAssignmentCompleted(entry)

    def filter(entry: TransferEntry): Boolean =
      sourceDomain.forall(_ == entry.transferData.sourceDomain) &&
        incompleteTransfer(entry) &&
        stakeholders.forall(_.exists(entry.transferData.contract.metadata.stakeholders))

    val values = transferDataMap.values
      .to(LazyList)
      .collect {
        case entry if filter(entry) => IncompleteTransferData.tryCreate(entry.transferData, validAt)
      }
      .sortBy(_.queryOffset)
      .take(limit.unwrap)

    Future.successful(values)
  }

  override def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, ReassignmentId, TargetDomainId)]] =
    // empty table: there are no transfers
    if (transferDataMap.isEmpty) Future.successful(None)
    else {
      def incompleteTransfer(entry: TransferEntry): Boolean =
        (entry.transferData.assignmentGlobalOffset.isEmpty ||
          entry.transferData.unassignmentGlobalOffset.isEmpty) &&
          entry.transferData.targetDomain == domain
      val incompleteTransfers = transferDataMap.values
        .to(LazyList)
        .filter(entry => incompleteTransfer(entry))
      // all transfers are complete
      if (incompleteTransfers.isEmpty) Future.successful(None)
      else {
        val incompleteTransfersOffsets = incompleteTransfers
          .mapFilter(entry =>
            (
              entry.transferData.assignmentGlobalOffset
                .orElse(entry.transferData.unassignmentGlobalOffset),
              entry.transferData.reassignmentId,
            ) match {
              case (Some(o), transferId) => Some((o, transferId))
              case (None, _) => None
            }
          )
        // only in-flight transfers
        if (incompleteTransfersOffsets.isEmpty) Future.successful(None)
        else {
          val default = (
            GlobalOffset.MaxValue,
            ReassignmentId(SourceDomainId(domain.unwrap), CantonTimestamp.MaxValue),
          )
          val minIncompleteTransferOffset =
            incompleteTransfersOffsets.minByOption(_._1).getOrElse(default)
          Future.successful(
            Some((minIncompleteTransferOffset._1, minIncompleteTransferOffset._2, domain))
          )
        }
      }
    }
}
