// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.syntax.either.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.IncompleteTransferData.TransferEventGlobalOffset
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import io.scalaland.chimney.dsl.*

/** Stores the data for a transfer that is incomplete, i.e., for which only the transfer-in or the transfer-out was
  * emitted on the multi-domain event log.
  *
  * If [[IncompleteTransferData.TransferEventGlobalOffset]] is a [[IncompleteTransferData.TransferOutEventGlobalOffset]],
  * it means that the transfer-out event was emitted before or at `queryOffset` and that transfer-in event was not yet
  * emitted or at an offset greater than `queryOffset`.
  * The same holds symmetrically for a [[IncompleteTransferData.TransferInEventGlobalOffset]].
  */
final case class IncompleteTransferData private (
    sourceProtocolVersion: SourceProtocolVersion,
    transferOutTimestamp: CantonTimestamp,
    transferOutRequestCounter: RequestCounter,
    transferOutRequest: FullTransferOutTree,
    transferOutDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResult: Option[DeliveredTransferOutResult],
    transferEventGlobalOffset: TransferEventGlobalOffset,
    queryOffset: GlobalOffset,
) {

  def sourceDomain: SourceDomainId = transferOutRequest.sourceDomain
  def targetDomain: TargetDomainId = transferOutRequest.targetDomain

  def transferOutGlobalOffset: Option[GlobalOffset] =
    transferEventGlobalOffset.transferOutGlobalOffset

  def transferInGlobalOffset: Option[GlobalOffset] =
    transferEventGlobalOffset.transferInGlobalOffset

  require(
    contract.contractId == transferOutRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${transferOutRequest.contractId} of the transfer-out request.",
  )

  def toTransferData: TransferData = this
    .into[TransferData]
    .withFieldComputed(
      _.transferGlobalOffset,
      _.transferEventGlobalOffset match {
        case IncompleteTransferData.TransferInEventGlobalOffset(globalOffset) =>
          Some(TransferData.TransferInGlobalOffset(globalOffset))
        case IncompleteTransferData.TransferOutEventGlobalOffset(globalOffset) =>
          Some(TransferData.TransferOutGlobalOffset(globalOffset))
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
        transferOutGlobalOffset = transferData.transferOutGlobalOffset,
        transferInGlobalOffset = transferData.transferInGlobalOffset,
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
    def transferOutGlobalOffset: Option[GlobalOffset]
    def transferInGlobalOffset: Option[GlobalOffset]
  }

  final case class TransferInEventGlobalOffset(globalOffset: GlobalOffset)
      extends TransferEventGlobalOffset {
    override def transferOutGlobalOffset: Option[GlobalOffset] = None

    override def transferInGlobalOffset: Option[GlobalOffset] = Some(globalOffset)
  }

  final case class TransferOutEventGlobalOffset(globalOffset: GlobalOffset)
      extends TransferEventGlobalOffset {
    override def transferOutGlobalOffset: Option[GlobalOffset] = Some(globalOffset)

    override def transferInGlobalOffset: Option[GlobalOffset] = None
  }

  object TransferEventGlobalOffset {
    private[transfer] def create(
        queryOffset: GlobalOffset,
        transferOutGlobalOffset: Option[GlobalOffset],
        transferInGlobalOffset: Option[GlobalOffset],
    ): Either[String, TransferEventGlobalOffset] =
      (transferOutGlobalOffset, transferInGlobalOffset) match {
        case (Some(out), None) if out <= queryOffset => Right(TransferOutEventGlobalOffset(out))

        case (None, Some(in)) if in <= queryOffset => Right(TransferInEventGlobalOffset(in))

        case (Some(out), Some(in)) if out <= queryOffset && queryOffset < in =>
          Right(TransferOutEventGlobalOffset(out))
        case (Some(out), Some(in)) if in <= queryOffset && queryOffset < out =>
          Right(TransferInEventGlobalOffset(in))

        case _ =>
          Left(
            s"Expecting incomplete transfer at offset $queryOffset, found out=${transferOutGlobalOffset} and in=${transferInGlobalOffset}"
          )
      }
  }
}
