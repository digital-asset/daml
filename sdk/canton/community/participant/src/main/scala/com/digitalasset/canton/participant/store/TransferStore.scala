// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse_
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.TransferData.*
import com.digitalasset.canton.participant.protocol.transfer.{IncompleteTransferData, TransferData}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateLookup
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.platform.indexer.parallel.ReassignmentOffsetPersistence
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{ReassignmentId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT, EitherTUtil, MonadUtil, OptionUtil}
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import monocle.macros.syntax.lens.*

import scala.concurrent.{ExecutionContext, Future}

trait TransferStore extends TransferLookup {
  import TransferStore.*

  /** Adds the transfer to the store.
    *
    * Calls to this method are idempotent, independent of the order.
    * Differences in [[protocol.transfer.TransferData!.transferOutResult]] between two calls are ignored
    * if the field is [[scala.None$]] in one of the calls. If applicable, the field content is merged.
    *
    * @throws java.lang.IllegalArgumentException if the transfer's target domain is not
    *                                            the domain this [[TransferStore]] belongs to.
    */
  def addTransfer(transferData: TransferData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.protocol.messages.ConfirmationResultMessage]] to the transfer data in the store,
    * provided that the transfer data has previously been stored.
    *
    * The same [[com.digitalasset.canton.protocol.messages.ConfirmationResultMessage]] can be added any number of times.
    * This includes transfer-out results that are in the [[protocol.transfer.TransferData!.transferOutResult]]
    * added with [[addTransfer]].
    *
    * @param transferOutResult The transfer-out result to add
    * @return [[TransferStore$.UnknownReassignmentId]] if the transfer has not previously been added with [[addTransfer]].
    *         [[TransferStore$.TransferOutResultAlreadyExists]] if a different transfer-out result for the same
    *         transfer request has been added before, including as part of [[addTransfer]].
    */
  def addTransferOutResult(transferOutResult: DeliveredTransferOutResult)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit]

  /** Adds the given offsets to the transfer data in the store, provided that the transfer data has previously been stored.
    *
    * The same offset can be added any number of times.
    */
  def addTransfersOffsets(offsets: Map[ReassignmentId, TransferGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.participant.GlobalOffset]] for the transfer events to the transfer data in
    * the store, provided that the transfer data has previously been stored.
    *
    * The same [[com.digitalasset.canton.participant.GlobalOffset]] can be added any number of times.
    */
  def addTransfersOffsets(
      events: Seq[(ReassignmentId, TransferGlobalOffset)]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransferStoreError, Unit] =
    for {
      preparedOffsets <- EitherT.fromEither[FutureUnlessShutdown](mergeTransferOffsets(events))
      _ <- addTransfersOffsets(preparedOffsets)
    } yield ()

  /** Marks the transfer as completed, i.e., a transfer-in request was committed.
    * If the transfer has already been completed then a [[TransferStore.TransferAlreadyCompleted]] is reported, and the
    * [[com.digitalasset.canton.participant.util.TimeOfChange]] of the completion is not changed from the old value.
    *
    * @param timeOfCompletion Provides the request counter and activeness time of the committed transfer-in request.
    */
  def completeTransfer(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit]

  /** Removes the transfer from the store,
    * when the transfer-out request is rejected or the transfer is pruned.
    */
  def deleteTransfer(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Removes all completions of transfers that have been triggered by requests with at least the given counter.
    * This method must not be called concurrently with [[completeTransfer]], but may be called concurrently with
    * [[addTransfer]] and [[addTransferOutResult]].
    *
    * Therefore, this method need not be linearizable w.r.t. [[completeTransfer]].
    * For example, if two requests `rc1` complete two transfers while [[deleteCompletionsSince]] is running for
    * some `rc <= rc1, rc2`, then there are no guarantees which of the completions of `rc1` and `rc2` remain.
    */
  def deleteCompletionsSince(criterionInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

object TransferStore {
  def transferStoreFor(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup
  ): TargetDomainId => Either[String, TransferStore] = (domainId: TargetDomainId) =>
    syncDomainPersistentStates.getAll
      .get(domainId.unwrap)
      .toRight(s"Unknown domain `${domainId.unwrap}`")
      .map(_.transferStore)

  def reassignmentOffsetPersistenceFor(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup
  )(implicit
      executionContext: ExecutionContext
  ): ReassignmentOffsetPersistence = new ReassignmentOffsetPersistence {
    override def persist(
        updates: Seq[(Update, Offset)],
        tracedLogger: TracedLogger,
    )(implicit traceContext: TraceContext): Future[Unit] =
      updates
        .collect {
          case (transferAccepted: Update.ReassignmentAccepted, offset)
              if transferAccepted.reassignmentInfo.isReassigningParticipant =>
            (transferAccepted, offset)
        }
        .groupBy { case (event, _) => event.reassignmentInfo.targetDomain }
        .toList
        .parTraverse_ { case (targetDomain, eventsForDomain) =>
          lazy val updates = eventsForDomain
            .map { case (event, offset) =>
              s"${event.reassignmentInfo.sourceDomain} ${event.reassignmentInfo.unassignId} (${event.reassignment}): $offset"
            }
            .mkString(", ")

          val res: EitherT[FutureUnlessShutdown, String, Unit] = for {
            transferStore <- EitherT
              .fromEither[FutureUnlessShutdown](
                transferStoreFor(syncDomainPersistentStates)(targetDomain)
              )
            offsets = eventsForDomain.map { case (reassignmentEvent, offset) =>
              val globalOffset = GlobalOffset.tryFromLong(offset.toLong)
              val transferOffset = reassignmentEvent.reassignment match {
                case _: Reassignment.Assign => TransferInGlobalOffset(globalOffset)
                case _: Reassignment.Unassign => TransferOutGlobalOffset(globalOffset)
              }
              ReassignmentId(
                reassignmentEvent.reassignmentInfo.sourceDomain,
                reassignmentEvent.reassignmentInfo.unassignId,
              ) -> transferOffset
            }
            _ = tracedLogger.debug(s"Updated global offsets for transfers: $updates")
            _ <- transferStore.addTransfersOffsets(offsets).leftMap(_.message)
          } yield ()

          EitherTUtil
            .toFutureUnlessShutdown(
              res.leftMap(err =>
                new RuntimeException(
                  s"Unable to update global offsets for transfers ($updates): $err"
                )
              )
            )
            .onShutdown(
              throw new RuntimeException(
                "Notification upon published transfer aborted due to shutdown"
              )
            )
        }
  }

  /** Merge the offsets corresponding to the same transfer id.
    * Returns an error in case of inconsistent offsets.
    */
  def mergeTransferOffsets(
      events: Seq[(ReassignmentId, TransferGlobalOffset)]
  ): Either[ConflictingGlobalOffsets, Map[ReassignmentId, TransferGlobalOffset]] = {
    type Acc = Map[ReassignmentId, TransferGlobalOffset]
    val zero: Acc = Map.empty[ReassignmentId, TransferGlobalOffset]

    MonadUtil.foldLeftM[Either[
      ConflictingGlobalOffsets,
      *,
    ], Acc, (ReassignmentId, TransferGlobalOffset)](
      zero,
      events,
    ) { case (acc, (reassignmentId, offset)) =>
      val newOffsetE = acc.get(reassignmentId) match {
        case Some(value) =>
          value.merge(offset).leftMap(ConflictingGlobalOffsets(reassignmentId, _))
        case None => Right(offset)
      }

      newOffsetE.map(newOffset => acc + (reassignmentId -> newOffset))
    }
  }

  sealed trait TransferStoreError extends Product with Serializable {
    def message: String
  }
  sealed trait TransferLookupError extends TransferStoreError {
    def cause: String
    def reassignmentId: ReassignmentId
    def message: String = s"Cannot lookup for transfer `$reassignmentId`: $cause"
  }

  final case class UnknownReassignmentId(reassignmentId: ReassignmentId)
      extends TransferLookupError {
    override def cause: String = "unknown reassignment id"
  }

  final case class TransferCompleted(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)
      extends TransferLookupError {
    override def cause: String = "transfer already completed"
  }

  final case class TransferDataAlreadyExists(old: TransferData, `new`: TransferData)
      extends TransferStoreError {
    def reassignmentId: ReassignmentId = old.reassignmentId

    override def message: String =
      s"Transfer data for transfer `$reassignmentId` already exists and differs from the new one"
  }

  final case class TransferOutResultAlreadyExists(
      reassignmentId: ReassignmentId,
      old: DeliveredTransferOutResult,
      `new`: DeliveredTransferOutResult,
  ) extends TransferStoreError {
    override def message: String =
      s"Transfer-out result for transfer `$reassignmentId` already exists and differs from the new one"
  }

  final case class TransferGlobalOffsetsMerge(
      reassignmentId: ReassignmentId,
      error: String,
  ) extends TransferStoreError {
    override def message: String =
      s"Unable to merge global offsets for transfer `$reassignmentId`: $error"
  }

  final case class ConflictingGlobalOffsets(reassignmentId: ReassignmentId, error: String)
      extends TransferStoreError {
    override def message: String = s"Conflicting global offsets for $reassignmentId: $error"
  }

  final case class TransferAlreadyCompleted(
      reassignmentId: ReassignmentId,
      newCompletion: TimeOfChange,
  ) extends TransferStoreError {
    override def message: String = s"Transfer `$reassignmentId` is already completed"
  }

  /** The data for a transfer and possible when the transfer was completed. */
  final case class TransferEntry(
      transferData: TransferData,
      timeOfCompletion: Option[TimeOfChange],
  ) {
    def isCompleted: Boolean = timeOfCompletion.nonEmpty

    def mergeWith(
        other: TransferEntry
    ): Checked[TransferDataAlreadyExists, TransferAlreadyCompleted, TransferEntry] =
      for {
        mergedData <- Checked.fromEither(
          transferData
            .mergeWith(other.transferData)
            .toRight(TransferDataAlreadyExists(transferData, other.transferData))
        )
        mergedToc <- OptionUtil
          .mergeEqual(timeOfCompletion, other.timeOfCompletion)
          .fold[
            Checked[TransferDataAlreadyExists, TransferAlreadyCompleted, Option[TimeOfChange]]
          ] {
            val thisToC =
              timeOfCompletion.getOrElse(
                throw new IllegalStateException("Time of completion should be defined")
              )
            val otherToC =
              other.timeOfCompletion.getOrElse(
                throw new IllegalStateException("Time of completion should be defined")
              )

            Checked.continueWithResult(
              TransferAlreadyCompleted(transferData.reassignmentId, otherToC),
              Some(thisToC),
            )
          }(Checked.result)
      } yield
        if ((mergedData eq transferData) && (mergedToc eq timeOfCompletion)) this
        else TransferEntry(mergedData, mergedToc)

    private[store] def addTransferOutResult(
        transferOutResult: DeliveredTransferOutResult
    ): Either[TransferOutResultAlreadyExists, TransferEntry] =
      transferData
        .addTransferOutResult(transferOutResult)
        .toRight {
          val old = transferData.transferOutResult.getOrElse(
            throw new IllegalStateException("Transfer-out result should not be empty")
          )
          TransferOutResultAlreadyExists(transferData.reassignmentId, old, transferOutResult)
        }
        .map(TransferEntry(_, timeOfCompletion))

    private[store] def addTransferOutGlobalOffset(
        offset: TransferGlobalOffset
    ): Either[TransferGlobalOffsetsMerge, TransferEntry] = {

      val newGlobalOffsetE = transferData.transferGlobalOffset
        .fold[Either[TransferGlobalOffsetsMerge, TransferGlobalOffset]](Right(offset))(
          _.merge(offset).leftMap(TransferGlobalOffsetsMerge(transferData.reassignmentId, _))
        )

      newGlobalOffsetE.map(newGlobalOffset =>
        this.focus(_.transferData.transferGlobalOffset).replace(Some(newGlobalOffset))
      )
    }

    def complete(
        timeOfChange: TimeOfChange
    ): Checked[TransferDataAlreadyExists, TransferAlreadyCompleted, TransferEntry] =
      mergeWith(TransferEntry(transferData, Some(timeOfChange)))

    def clearCompletion: TransferEntry = TransferEntry(transferData, None)
  }
}

trait TransferLookup {
  import TransferStore.*

  /** Looks up the given in-flight transfer and returns the data associated with the transfer.
    *
    * @return [[scala.Left$]]([[TransferStore.UnknownReassignmentId]]) if the transfer is unknown;
    *         [[scala.Left$]]([[TransferStore.TransferCompleted]]) if the transfer has already been completed.
    */
  def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferLookupError, TransferData]

  /** Find utility to look for in-flight transfers.
    * Results need not be consistent with [[lookup]].
    */
  def find(
      filterSource: Option[SourceDomainId],
      filterRequestTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]]

  /** Find utility to look for in-flight transfers.
    * Transfers are ordered by the tuple (request timestamp, source domain ID), ie transfers are ordered by request timestamps
    * and ties are broken with lexicographic ordering on domain IDs.
    *
    * The ordering here has been chosen to allow a participant to fetch all the pending transfers. The ordering has to
    * be consistent accross calls and uniquely identify a pending transfer, but is otherwise arbitrary.
    *
    * @param requestAfter optionally, specify a strict lower bound for the transfers returned, according to the
    *                     (request timestamp, source domain ID) ordering
    * @param limit limit the number of results
    */
  def findAfter(requestAfter: Option[(CantonTimestamp, SourceDomainId)], limit: Int)(implicit
      traceContext: TraceContext
  ): Future[Seq[TransferData]]

  /** Find utility to look for incomplete transfers.
    * Transfers are ordered by global offset.
    *
    * A transfer `t` is considered as incomplete at offset `validAt` if only one of the two transfer events
    * was emitted on the multi-domain event log at `validAt`. That is, one of the following hold:
    *   1. Only transfer-out was emitted
    *       - `t.transferOutGlobalOffset` is smaller or equal to `validAt`
    *       - `t.transferInGlobalOffset` is null or greater than `validAt`
    *   2. Only transfer-in was emitted
    *       - `t.transferInGlobalOffset` is smaller or equal to `validAt`
    *       - `t.transferOutGlobalOffset` is null or greater than `validAt`
    *
    * In particular, for a transfer to be considered incomplete at `validAt`, then exactly one of the two offsets
    * (transferOutGlobalOffset, transferInGlobalOffset) is not null and smaller or equal to `validAt`.
    *
    * @param sourceDomain if empty, select only transfers whose source domain matches the given one
    * @param validAt select only transfers that are successfully transferred-out
    * @param stakeholders if non-empty, select only transfers of contracts whose set of stakeholders
    *                     intersects `stakeholders`.
    * @param limit limit the number of results
    */
  def findIncomplete(
      sourceDomain: Option[SourceDomainId],
      validAt: GlobalOffset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): Future[Seq[IncompleteTransferData]]

  /** Find utility to look for the earliest incomplete transfer w.r.t. the ledger end.
    * If an incomplete transfer exists, the method returns the global offset of the incomplete transfer for either the
    * transfer-out or the transfer-in, whichever of these is not null, the reassignment id and the target domain id.
    * It returns None if there is no incomplete transfer (either because all transfers are complete or are in-flight,
    * or because there are no transfers), or the transfer table is empty.
    */
  def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, ReassignmentId, TargetDomainId)]]
}
