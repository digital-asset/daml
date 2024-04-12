// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.TransferData.TransferGlobalOffset
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
  TransferId,
}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import com.digitalasset.canton.{RequestCounter, TransferCounter}

/** Stores the data for a transfer that needs to be passed from the source domain to the target domain. */
final case class TransferData(
    sourceProtocolVersion: SourceProtocolVersion,
    transferOutTimestamp: CantonTimestamp,
    transferOutRequestCounter: RequestCounter,
    transferOutRequest: FullTransferOutTree,
    transferOutDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResult: Option[DeliveredTransferOutResult],
    transferGlobalOffset: Option[TransferGlobalOffset],
) {

  require(
    contract.contractId == transferOutRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${transferOutRequest.contractId} of the transfer-out request.",
  )

  def transferOutGlobalOffset: Option[GlobalOffset] = transferGlobalOffset.flatMap(_.out)
  def transferInGlobalOffset: Option[GlobalOffset] = transferGlobalOffset.flatMap(_.in)

  def targetDomain: TargetDomainId = transferOutRequest.targetDomain

  def sourceDomain: SourceDomainId = transferOutRequest.sourceDomain

  def transferId: TransferId = TransferId(transferOutRequest.sourceDomain, transferOutTimestamp)

  def sourceMediator: MediatorGroupRecipient = transferOutRequest.mediator

  def transferCounter: TransferCounter = transferOutRequest.transferCounter

  def addTransferOutResult(result: DeliveredTransferOutResult): Option[TransferData] =
    mergeTransferOutResult(Some(result))

  def mergeWith(other: TransferData): Option[TransferData] = {
    if (this eq other) Some(this)
    else
      other match {
        case TransferData(
              `sourceProtocolVersion`,
              `transferOutTimestamp`,
              `transferOutRequestCounter`,
              `transferOutRequest`,
              `transferOutDecisionTime`,
              `contract`,
              `creatingTransactionId`,
              otherResult,
              otherTransferGlobalOffset,
            ) =>
          mergeTransferOutResult(otherResult)
            .flatMap(_.mergeTransferGlobalOffset(otherTransferGlobalOffset))
        case _ => None
      }
  }

  private[this] def mergeTransferOutResult(
      result: Option[DeliveredTransferOutResult]
  ): Option[TransferData] = {
    val oldResult = this.transferOutResult
    OptionUtil
      .mergeEqual(oldResult, result)
      .map(merged => if (merged eq oldResult) this else this.copy(transferOutResult = merged))
  }

  private def mergeTransferGlobalOffset(
      offset: Option[TransferGlobalOffset]
  ): Option[TransferData] = {
    val oldResult = this.transferGlobalOffset
    OptionUtil
      .mergeEqual(oldResult, offset)
      .map(merged => if (merged eq oldResult) this else this.copy(transferGlobalOffset = merged))
  }

}

object TransferData {
  sealed trait TransferGlobalOffset extends Product with Serializable {
    def merge(other: TransferGlobalOffset): Either[String, TransferGlobalOffset]

    def out: Option[GlobalOffset]
    def in: Option[GlobalOffset]
  }

  object TransferGlobalOffset {
    def create(
        out: Option[GlobalOffset],
        in: Option[GlobalOffset],
    ): Either[String, Option[TransferGlobalOffset]] =
      (out, in) match {
        case (Some(out), Some(in)) => TransferGlobalOffsets.create(out, in).map(Some(_))
        case (Some(out), None) => Right(Some(TransferOutGlobalOffset(out)))
        case (None, Some(in)) => Right(Some(TransferInGlobalOffset(in)))
        case (None, None) => Right(None)
      }
  }

  final case class TransferOutGlobalOffset(offset: GlobalOffset) extends TransferGlobalOffset {
    override def merge(
        other: TransferGlobalOffset
    ): Either[String, TransferGlobalOffset] =
      other match {
        case TransferOutGlobalOffset(newOut) =>
          Either.cond(
            offset == newOut,
            this,
            s"Unable to merge transfer-out offsets $offset and $newOut",
          )
        case TransferInGlobalOffset(newIn) => TransferGlobalOffsets.create(offset, newIn)
        case offsets @ TransferGlobalOffsets(newOut, _) =>
          Either.cond(
            offset == newOut,
            offsets,
            s"Unable to merge transfer-out offsets $offset and $newOut",
          )
      }

    override def out: Option[GlobalOffset] = Some(offset)
    override def in: Option[GlobalOffset] = None
  }

  final case class TransferInGlobalOffset(offset: GlobalOffset) extends TransferGlobalOffset {
    override def merge(
        other: TransferGlobalOffset
    ): Either[String, TransferGlobalOffset] =
      other match {
        case TransferInGlobalOffset(newIn) =>
          Either.cond(
            offset == newIn,
            this,
            s"Unable to merge transfer-in offsets $offset and $newIn",
          )
        case TransferOutGlobalOffset(newOut) =>
          TransferGlobalOffsets.create(newOut, offset)
        case offsets @ TransferGlobalOffsets(_, newIn) =>
          Either.cond(
            offset == newIn,
            offsets,
            s"Unable to merge transfer-in offsets $offset and $newIn",
          )
      }

    override def out: Option[GlobalOffset] = None
    override def in: Option[GlobalOffset] = Some(offset)
  }

  final case class TransferGlobalOffsets private (outOffset: GlobalOffset, inOffset: GlobalOffset)
      extends TransferGlobalOffset {
    require(out != in, s"Out and in offsets should be different; got $out")

    override def merge(
        other: TransferGlobalOffset
    ): Either[String, TransferGlobalOffset] =
      other match {
        case TransferOutGlobalOffset(newOut) =>
          Either.cond(
            newOut == outOffset,
            this,
            s"Unable to merge transfer-out offsets $out and $newOut",
          )
        case TransferInGlobalOffset(newIn) =>
          Either.cond(
            newIn == inOffset,
            this,
            s"Unable to merge transfer-in offsets $in and $newIn",
          )
        case TransferGlobalOffsets(newOut, newIn) =>
          Either.cond(
            newOut == outOffset && newIn == inOffset,
            this,
            s"Unable to merge transfer offsets ($out, $in) and ($newOut, $newIn)",
          )
      }

    override def out: Option[GlobalOffset] = Some(outOffset)
    override def in: Option[GlobalOffset] = Some(inOffset)
  }

  object TransferGlobalOffsets {
    def create(out: GlobalOffset, in: GlobalOffset): Either[String, TransferGlobalOffsets] =
      Either.cond(
        out != in,
        TransferGlobalOffsets(out, in),
        s"Out and in offsets should be different but got $out",
      )
  }
}
