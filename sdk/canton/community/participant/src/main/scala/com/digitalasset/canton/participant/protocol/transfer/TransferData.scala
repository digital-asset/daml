// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.TransferData.TransferGlobalOffset
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{
  ReassignmentId,
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import com.digitalasset.canton.{ReassignmentCounter, RequestCounter}

/** Stores the data for a transfer that needs to be passed from the source domain to the target domain. */
final case class TransferData(
    sourceProtocolVersion: SourceProtocolVersion,
    unassignmentTs: CantonTimestamp,
    unassignmentRequestCounter: RequestCounter,
    unassignmentRequest: FullUnassignmentTree,
    unassignmentDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    unassignmentResult: Option[DeliveredUnassignmentResult],
    transferGlobalOffset: Option[TransferGlobalOffset],
) {

  require(
    contract.contractId == unassignmentRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${unassignmentRequest.contractId} of the unassignment request.",
  )

  def unassignmentGlobalOffset: Option[GlobalOffset] = transferGlobalOffset.flatMap(_.out)
  def assignmentGlobalOffset: Option[GlobalOffset] = transferGlobalOffset.flatMap(_.in)

  def targetDomain: TargetDomainId = unassignmentRequest.targetDomain

  def sourceDomain: SourceDomainId = unassignmentRequest.sourceDomain

  def reassignmentId: ReassignmentId =
    ReassignmentId(unassignmentRequest.sourceDomain, unassignmentTs)

  def sourceMediator: MediatorGroupRecipient = unassignmentRequest.mediator

  def reassignmentCounter: ReassignmentCounter = unassignmentRequest.reassignmentCounter

  def addUnassignmentResult(result: DeliveredUnassignmentResult): Option[TransferData] =
    mergeUnassignmentResult(Some(result))

  def mergeWith(other: TransferData): Option[TransferData] =
    if (this eq other) Some(this)
    else
      other match {
        case TransferData(
              `sourceProtocolVersion`,
              `unassignmentTs`,
              `unassignmentRequestCounter`,
              `unassignmentRequest`,
              `unassignmentDecisionTime`,
              `contract`,
              `creatingTransactionId`,
              otherResult,
              otherTransferGlobalOffset,
            ) =>
          mergeUnassignmentResult(otherResult)
            .flatMap(_.mergeTransferGlobalOffset(otherTransferGlobalOffset))
        case _ => None
      }

  private[this] def mergeUnassignmentResult(
      result: Option[DeliveredUnassignmentResult]
  ): Option[TransferData] = {
    val oldResult = this.unassignmentResult
    OptionUtil
      .mergeEqual(oldResult, result)
      .map(merged => if (merged eq oldResult) this else this.copy(unassignmentResult = merged))
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
        case (Some(out), Some(in)) => ReassignmentGlobalOffsets.create(out, in).map(Some(_))
        case (Some(out), None) => Right(Some(UnassignmentGlobalOffset(out)))
        case (None, Some(in)) => Right(Some(AssignmentGlobalOffset(in)))
        case (None, None) => Right(None)
      }
  }

  final case class UnassignmentGlobalOffset(offset: GlobalOffset) extends TransferGlobalOffset {
    override def merge(
        other: TransferGlobalOffset
    ): Either[String, TransferGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newOut) =>
          Either.cond(
            offset == newOut,
            this,
            s"Unable to merge unassignment offsets $offset and $newOut",
          )
        case AssignmentGlobalOffset(newIn) => ReassignmentGlobalOffsets.create(offset, newIn)
        case offsets @ ReassignmentGlobalOffsets(newOut, _) =>
          Either.cond(
            offset == newOut,
            offsets,
            s"Unable to merge unassignment offsets $offset and $newOut",
          )
      }

    override def out: Option[GlobalOffset] = Some(offset)
    override def in: Option[GlobalOffset] = None
  }

  final case class AssignmentGlobalOffset(offset: GlobalOffset) extends TransferGlobalOffset {
    override def merge(
        other: TransferGlobalOffset
    ): Either[String, TransferGlobalOffset] =
      other match {
        case AssignmentGlobalOffset(newIn) =>
          Either.cond(
            offset == newIn,
            this,
            s"Unable to merge assignment offsets $offset and $newIn",
          )
        case UnassignmentGlobalOffset(newOut) =>
          ReassignmentGlobalOffsets.create(newOut, offset)
        case offsets @ ReassignmentGlobalOffsets(_, newIn) =>
          Either.cond(
            offset == newIn,
            offsets,
            s"Unable to merge assignment offsets $offset and $newIn",
          )
      }

    override def out: Option[GlobalOffset] = None
    override def in: Option[GlobalOffset] = Some(offset)
  }

  final case class ReassignmentGlobalOffsets private (
      unassignment: GlobalOffset,
      assignment: GlobalOffset,
  ) extends TransferGlobalOffset {
    // TODO(#21081) Look for out, in through the code base
    // TODO(#21081) Look for transferred
    require(out != in, s"Out and in offsets should be different; got $out")

    override def merge(
        other: TransferGlobalOffset
    ): Either[String, TransferGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newOut) =>
          Either.cond(
            newOut == unassignment,
            this,
            s"Unable to merge unassignment offsets $out and $newOut",
          )
        case AssignmentGlobalOffset(newIn) =>
          Either.cond(
            newIn == assignment,
            this,
            s"Unable to merge assignment offsets $in and $newIn",
          )
        case ReassignmentGlobalOffsets(newOut, newIn) =>
          Either.cond(
            newOut == unassignment && newIn == assignment,
            this,
            s"Unable to merge reassignment offsets ($out, $in) and ($newOut, $newIn)",
          )
      }

    override def out: Option[GlobalOffset] = Some(unassignment)
    override def in: Option[GlobalOffset] = Some(assignment)
  }

  object ReassignmentGlobalOffsets {
    def create(out: GlobalOffset, in: GlobalOffset): Either[String, ReassignmentGlobalOffsets] =
      Either.cond(
        out != in,
        ReassignmentGlobalOffsets(out, in),
        s"Out and in offsets should be different but got $out",
      )
  }
}
