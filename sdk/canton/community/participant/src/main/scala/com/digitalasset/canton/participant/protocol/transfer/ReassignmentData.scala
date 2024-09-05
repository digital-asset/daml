// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentData.ReassignmentGlobalOffset
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

/** Stores the data for a reassignment that needs to be passed from the source domain to the target domain. */
final case class ReassignmentData(
    sourceProtocolVersion: SourceProtocolVersion,
    unassignmentTs: CantonTimestamp,
    unassignmentRequestCounter: RequestCounter,
    unassignmentRequest: FullUnassignmentTree,
    unassignmentDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    unassignmentResult: Option[DeliveredUnassignmentResult],
    reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
) {

  require(
    contract.contractId == unassignmentRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${unassignmentRequest.contractId} of the unassignment request.",
  )

  def unassignmentGlobalOffset: Option[GlobalOffset] =
    reassignmentGlobalOffset.flatMap(_.unassignment)
  def assignmentGlobalOffset: Option[GlobalOffset] = reassignmentGlobalOffset.flatMap(_.assignment)

  def targetDomain: TargetDomainId = unassignmentRequest.targetDomain

  def sourceDomain: SourceDomainId = unassignmentRequest.sourceDomain

  def reassignmentId: ReassignmentId =
    ReassignmentId(unassignmentRequest.sourceDomain, unassignmentTs)

  def sourceMediator: MediatorGroupRecipient = unassignmentRequest.mediator

  def reassignmentCounter: ReassignmentCounter = unassignmentRequest.reassignmentCounter

  def addUnassignmentResult(result: DeliveredUnassignmentResult): Option[ReassignmentData] =
    mergeUnassignmentResult(Some(result))

  def mergeWith(other: ReassignmentData): Option[ReassignmentData] =
    if (this eq other) Some(this)
    else
      other match {
        case ReassignmentData(
              `sourceProtocolVersion`,
              `unassignmentTs`,
              `unassignmentRequestCounter`,
              `unassignmentRequest`,
              `unassignmentDecisionTime`,
              `contract`,
              `creatingTransactionId`,
              otherResult,
              otherReassignmentGlobalOffset,
            ) =>
          mergeUnassignmentResult(otherResult)
            .flatMap(_.mergeReassignmentGlobalOffset(otherReassignmentGlobalOffset))
        case _ => None
      }

  private[this] def mergeUnassignmentResult(
      result: Option[DeliveredUnassignmentResult]
  ): Option[ReassignmentData] = {
    val oldResult = this.unassignmentResult
    OptionUtil
      .mergeEqual(oldResult, result)
      .map(merged => if (merged eq oldResult) this else this.copy(unassignmentResult = merged))
  }

  private def mergeReassignmentGlobalOffset(
      offset: Option[ReassignmentGlobalOffset]
  ): Option[ReassignmentData] = {
    val oldResult = this.reassignmentGlobalOffset
    OptionUtil
      .mergeEqual(oldResult, offset)
      .map(merged =>
        if (merged eq oldResult) this else this.copy(reassignmentGlobalOffset = merged)
      )
  }

}

object ReassignmentData {
  sealed trait ReassignmentGlobalOffset extends Product with Serializable {
    def merge(other: ReassignmentGlobalOffset): Either[String, ReassignmentGlobalOffset]

    def unassignment: Option[GlobalOffset]
    def assignment: Option[GlobalOffset]
  }

  object ReassignmentGlobalOffset {
    def create(
        unassignment: Option[GlobalOffset],
        assignment: Option[GlobalOffset],
    ): Either[String, Option[ReassignmentGlobalOffset]] =
      (unassignment, assignment) match {
        case (Some(unassignment), Some(assignment)) =>
          ReassignmentGlobalOffsets.create(unassignment, assignment).map(Some(_))
        case (Some(unassignment), None) => Right(Some(UnassignmentGlobalOffset(unassignment)))
        case (None, Some(assignment)) => Right(Some(AssignmentGlobalOffset(assignment)))
        case (None, None) => Right(None)
      }
  }

  final case class UnassignmentGlobalOffset(offset: GlobalOffset) extends ReassignmentGlobalOffset {
    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newUnassignment) =>
          Either.cond(
            offset == newUnassignment,
            this,
            s"Unable to merge unassignment offsets $offset and $newUnassignment",
          )
        case AssignmentGlobalOffset(newAssignment) =>
          ReassignmentGlobalOffsets.create(offset, newAssignment)
        case offsets @ ReassignmentGlobalOffsets(newUnassignment, _) =>
          Either.cond(
            offset == newUnassignment,
            offsets,
            s"Unable to merge unassignment offsets $offset and $newUnassignment",
          )
      }

    override def unassignment: Option[GlobalOffset] = Some(offset)
    override def assignment: Option[GlobalOffset] = None
  }

  final case class AssignmentGlobalOffset(offset: GlobalOffset) extends ReassignmentGlobalOffset {
    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case AssignmentGlobalOffset(newAssignment) =>
          Either.cond(
            offset == newAssignment,
            this,
            s"Unable to merge assignment offsets $offset and $newAssignment",
          )
        case UnassignmentGlobalOffset(newUnassignment) =>
          ReassignmentGlobalOffsets.create(newUnassignment, offset)
        case offsets @ ReassignmentGlobalOffsets(_, newAssignment) =>
          Either.cond(
            offset == newAssignment,
            offsets,
            s"Unable to merge assignment offsets $offset and $newAssignment",
          )
      }

    override def unassignment: Option[GlobalOffset] = None
    override def assignment: Option[GlobalOffset] = Some(offset)
  }

  final case class ReassignmentGlobalOffsets private (
      unassignmentOffset: GlobalOffset,
      assignmentOffset: GlobalOffset,
  ) extends ReassignmentGlobalOffset {
    // TODO(#21081) Look for out, in through the code base
    // TODO(#21081) Look for transferred
    require(
      unassignmentOffset != assignmentOffset,
      s"Unassignment and assignment offsets should be different; got $unassignmentOffset",
    )

    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newUnassignment) =>
          Either.cond(
            newUnassignment == unassignmentOffset,
            this,
            s"Unable to merge unassignment offsets $unassignment and $newUnassignment",
          )
        case AssignmentGlobalOffset(newAssignment) =>
          Either.cond(
            newAssignment == assignmentOffset,
            this,
            s"Unable to merge assignment offsets $assignment and $newAssignment",
          )
        case ReassignmentGlobalOffsets(newUnassignment, newAssignment) =>
          Either.cond(
            newUnassignment == unassignmentOffset && newAssignment == assignmentOffset,
            this,
            s"Unable to merge reassignment offsets ($unassignment, $assignment) and ($newUnassignment, $newAssignment)",
          )
      }

    override def unassignment: Option[GlobalOffset] = Some(unassignmentOffset)
    override def assignment: Option[GlobalOffset] = Some(assignmentOffset)
  }

  object ReassignmentGlobalOffsets {
    def create(
        unassignment: GlobalOffset,
        assignment: GlobalOffset,
    ): Either[String, ReassignmentGlobalOffsets] =
      Either.cond(
        unassignment != assignment,
        ReassignmentGlobalOffsets(unassignment, assignment),
        s"Unassignment and assignment offsets should be different but got $unassignment",
      )
  }
}
