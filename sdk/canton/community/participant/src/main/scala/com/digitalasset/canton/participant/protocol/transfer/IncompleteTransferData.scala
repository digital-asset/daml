// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.syntax.either.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.IncompleteTransferData.TransferEventGlobalOffset
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import io.scalaland.chimney.dsl.*

/** Stores the data for a transfer that is incomplete, i.e., for which only the assignment or the unassignment was
  * emitted on the multi-domain event log.
  *
  * If [[IncompleteTransferData.TransferEventGlobalOffset]] is a [[IncompleteTransferData.UnassignmentEventGlobalOffset]],
  * it means that the unassignment event was emitted before or at `queryOffset` and that assigned event was not yet
  * emitted or at an offset greater than `queryOffset`.
  * The same holds symmetrically for a [[IncompleteTransferData.AssignmentEventGlobalOffset]].
  */
final case class IncompleteTransferData private (
    sourceProtocolVersion: SourceProtocolVersion,
    unassignmentTs: CantonTimestamp,
    unassignmentRequestCounter: RequestCounter,
    unassignmentRequest: FullUnassignmentTree,
    unassignmentDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    unassignmentResult: Option[DeliveredUnassignmentResult],
    transferEventGlobalOffset: TransferEventGlobalOffset,
    queryOffset: GlobalOffset,
) {

  def sourceDomain: SourceDomainId = unassignmentRequest.sourceDomain
  def targetDomain: TargetDomainId = unassignmentRequest.targetDomain

  def unassignmentGlobalOffset: Option[GlobalOffset] =
    transferEventGlobalOffset.unassignmentGlobalOffset

  def assignmentGlobalOffset: Option[GlobalOffset] =
    transferEventGlobalOffset.assignmentGlobalOffset

  require(
    contract.contractId == unassignmentRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${unassignmentRequest.contractId} of the unassignment request.",
  )

  def toTransferData: TransferData = this
    .into[TransferData]
    .withFieldComputed(
      _.transferGlobalOffset,
      _.transferEventGlobalOffset match {
        case IncompleteTransferData.AssignmentEventGlobalOffset(globalOffset) =>
          Some(TransferData.AssignmentGlobalOffset(globalOffset))
        case IncompleteTransferData.UnassignmentEventGlobalOffset(globalOffset) =>
          Some(TransferData.UnassignmentGlobalOffset(globalOffset))
      },
    )
    .transform
}

object IncompleteTransferData {
  def create(
      transferData: TransferData,
      queryOffset: GlobalOffset,
  ): Either[String, IncompleteTransferData] = {
    val transferEventGlobalOffsetE: Either[String, TransferEventGlobalOffset] =
      TransferEventGlobalOffset.create(
        queryOffset = queryOffset,
        unassignmentGlobalOffset = transferData.unassignmentGlobalOffset,
        assignmentGlobalOffset = transferData.assignmentGlobalOffset,
      )

    transferEventGlobalOffsetE.map { transferEventGlobalOffset =>
      transferData
        .into[IncompleteTransferData]
        .withFieldConst(_.queryOffset, queryOffset)
        .withFieldConst(_.transferEventGlobalOffset, transferEventGlobalOffset)
        .transform
    }
  }

  def tryCreate(transferData: TransferData, queryOffset: GlobalOffset): IncompleteTransferData =
    create(transferData, queryOffset).valueOr(err =>
      throw new IllegalArgumentException(s"Unable to create IncompleteTransferData: $err")
    )

  sealed trait TransferEventGlobalOffset {
    def globalOffset: GlobalOffset
    def unassignmentGlobalOffset: Option[GlobalOffset]
    def assignmentGlobalOffset: Option[GlobalOffset]
  }

  final case class AssignmentEventGlobalOffset(globalOffset: GlobalOffset)
      extends TransferEventGlobalOffset {
    override def unassignmentGlobalOffset: Option[GlobalOffset] = None

    override def assignmentGlobalOffset: Option[GlobalOffset] = Some(globalOffset)
  }

  final case class UnassignmentEventGlobalOffset(globalOffset: GlobalOffset)
      extends TransferEventGlobalOffset {
    override def unassignmentGlobalOffset: Option[GlobalOffset] = Some(globalOffset)

    override def assignmentGlobalOffset: Option[GlobalOffset] = None
  }

  object TransferEventGlobalOffset {
    private[transfer] def create(
        queryOffset: GlobalOffset,
        unassignmentGlobalOffset: Option[GlobalOffset],
        assignmentGlobalOffset: Option[GlobalOffset],
    ): Either[String, TransferEventGlobalOffset] =
      (unassignmentGlobalOffset, assignmentGlobalOffset) match {
        case (Some(unassignment), None) if unassignment <= queryOffset =>
          Right(UnassignmentEventGlobalOffset(unassignment))

        case (None, Some(assignment)) if assignment <= queryOffset =>
          Right(AssignmentEventGlobalOffset(assignment))

        case (Some(unassignment), Some(assignment))
            if unassignment <= queryOffset && queryOffset < assignment =>
          Right(UnassignmentEventGlobalOffset(unassignment))
        case (Some(unassignment), Some(assignment))
            if assignment <= queryOffset && queryOffset < unassignment =>
          Right(AssignmentEventGlobalOffset(assignment))

        case _ =>
          Left(
            s"Expecting incomplete transfer at offset $queryOffset, found out=$unassignmentGlobalOffset and in=$assignmentGlobalOffset"
          )
      }
  }
}
