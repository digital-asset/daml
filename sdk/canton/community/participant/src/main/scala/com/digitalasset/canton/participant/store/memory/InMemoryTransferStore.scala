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
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.{IncompleteTransferData, TransferData}
import com.digitalasset.canton.participant.store.TransferStore
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId, TransferId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
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

  private[this] val transferDataMap: TrieMap[TransferId, TransferEntry] =
    new TrieMap[TransferId, TransferEntry]

  override def addTransfer(
      transferData: TransferData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] = {
    ErrorUtil.requireArgument(
      transferData.targetDomain == domain,
      s"Domain ${domain.unwrap.unwrap}: Transfer store cannot store transfer for domain ${transferData.targetDomain.unwrap.unwrap}",
    )

    val transferId = transferData.transferId
    val newEntry = TransferEntry(transferData, None)

    val result: Either[TransferDataAlreadyExists, Unit] = MapsUtil
      .updateWithConcurrentlyM_[Checked[
        TransferDataAlreadyExists,
        TransferAlreadyCompleted,
        *,
      ], TransferId, TransferEntry](
        transferDataMap,
        transferId,
        Checked.result(newEntry),
        _.mergeWith(newEntry),
      )
      .toEither

    EitherT(Future.successful(result))
  }

  private def editTransferEntry(
      transferId: TransferId,
      updateEntry: TransferEntry => Either[TransferStoreError, TransferEntry],
  ): EitherT[Future, TransferStoreError, Unit] = {
    val res =
      MapsUtil.updateWithConcurrentlyM_[Either[TransferStoreError, *], TransferId, TransferEntry](
        transferDataMap,
        transferId,
        Left(UnknownTransferId(transferId)),
        updateEntry,
      )

    EitherT(Future.successful(res))
  }

  override def addTransferOutResult(
      transferOutResult: DeliveredTransferOutResult
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] = {
    val transferId = transferOutResult.transferId
    editTransferEntry(transferId, _.addTransferOutResult(transferOutResult))
  }

  override def addTransfersOffsets(offsets: Map[TransferId, TransferData.TransferGlobalOffset])(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] = offsets.toList
    .parTraverse_ { case (transferId, newGlobalOffset) =>
      editTransferEntry(transferId, _.addTransferOutGlobalOffset(newGlobalOffset))
    }
    .mapK(FutureUnlessShutdown.outcomeK)

  override def completeTransfer(transferId: TransferId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit] =
    CheckedT(Future.successful {
      val result = MapsUtil
        .updateWithConcurrentlyChecked_[
          TransferStoreError,
          TransferAlreadyCompleted,
          TransferId,
          TransferEntry,
        ](
          transferDataMap,
          transferId,
          Checked.abort(UnknownTransferId(transferId)),
          _.complete(timeOfCompletion),
        )
      result.toResult(())
    })

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful {
    transferDataMap.foreach { case (transferId, entry) =>
      val entryTimeOfCompletion = entry.timeOfCompletion

      // Standard retry loop for clearing the completion in case the entry is updated concurrently.
      // Ensures progress as one writer succeeds in each iteration.
      @tailrec def clearCompletionCAS(): Unit = transferDataMap.get(transferId) match {
        case None =>
          () // The transfer ID has been deleted in the meantime so there's no completion to be cleared any more.
        case Some(newEntry) =>
          val newTimeOfCompletion = newEntry.timeOfCompletion
          if (newTimeOfCompletion.isDefined) {
            if (newTimeOfCompletion != entryTimeOfCompletion)
              throw new ConcurrentModificationException(
                s"Completion of transfer $transferId modified from $entryTimeOfCompletion to $newTimeOfCompletion while deleting completions."
              )
            val replaced = transferDataMap.replace(transferId, newEntry, newEntry.clearCompletion)
            if (!replaced) clearCompletionCAS()
          }
      }

      /* First use the entry from the read-only snapshot the iteration goes over
       * If the entry's completion must be cleared in the snapshot but the entry has meanwhile been modified in the table
       * (e.g., a transfer-out result has been added), then clear the table's entry.
       */
      if (entry.timeOfCompletion.exists(_.rc >= criterionInclusive)) {
        val replaced = transferDataMap.replace(transferId, entry, entry.clearCompletion)
        if (!replaced) clearCompletionCAS()
      }
    }
  }

  override def lookup(
      transferId: TransferId
  )(implicit traceContext: TraceContext): EitherT[Future, TransferLookupError, TransferData] =
    EitherT(Future.successful {
      for {
        entry <- transferDataMap.get(transferId).toRight(UnknownTransferId(transferId))
        _ <- entry.timeOfCompletion.map(TransferCompleted(transferId, _)).toLeft(())
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
        entry.timeOfCompletion.isEmpty && // Always filter out completed transfer-in
          filterSource.forall(source => entry.transferData.sourceDomain == source) &&
          filterTimestamp.forall(ts => entry.transferData.transferId.transferOutTimestamp == ts) &&
          filterSubmitter.forall(party => entry.transferData.transferOutRequest.submitter == party)

      transferDataMap.values.to(LazyList).filter(filter).take(limit).map(_.transferData)
    }

  override def deleteTransfer(
      transferId: TransferId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    transferDataMap.remove(transferId).discard
    Future.unit
  }

  override def findAfter(requestAfter: Option[(CantonTimestamp, SourceDomainId)], limit: Int)(
      implicit traceContext: TraceContext
  ): Future[Seq[TransferData]] = Future.successful {
    def filter(entry: TransferEntry): Boolean =
      entry.timeOfCompletion.isEmpty && // Always filter out completed transfer-in
        requestAfter.forall(ts =>
          (entry.transferData.transferId.transferOutTimestamp, entry.transferData.sourceDomain) > ts
        )

    transferDataMap.values
      .to(LazyList)
      .filter(filter)
      .map(_.transferData)
      .sortBy(t => (t.transferId.transferOutTimestamp, t.transferId.sourceDomain))(
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
    def onlyTransferOutCompleted(entry: TransferEntry): Boolean =
      entry.transferData.transferOutGlobalOffset.exists(_ <= validAt) &&
        entry.transferData.transferInGlobalOffset.forall(_ > validAt)

    def onlyTransferInCompleted(entry: TransferEntry): Boolean =
      entry.transferData.transferInGlobalOffset.exists(_ <= validAt) &&
        entry.transferData.transferOutGlobalOffset.forall(_ > validAt)

    def incompleteTransfer(entry: TransferEntry): Boolean =
      onlyTransferOutCompleted(entry) || onlyTransferInCompleted(entry)

    def filter(entry: TransferEntry): Boolean = {
      sourceDomain.forall(_ == entry.transferData.sourceDomain) &&
      incompleteTransfer(entry) &&
      stakeholders.forall(_.exists(entry.transferData.contract.metadata.stakeholders))
    }

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
  ): Future[Option[(GlobalOffset, TransferId, TargetDomainId)]] = {
    // empty table: there are no transfers
    if (transferDataMap.isEmpty) Future.successful(None)
    else {
      def incompleteTransfer(entry: TransferEntry): Boolean =
        (entry.transferData.transferInGlobalOffset.isEmpty ||
          entry.transferData.transferOutGlobalOffset.isEmpty) &&
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
              entry.transferData.transferInGlobalOffset
                .orElse(entry.transferData.transferOutGlobalOffset),
              entry.transferData.transferId,
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
            TransferId(SourceDomainId(domain.unwrap), CantonTimestamp.MaxValue),
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
}
