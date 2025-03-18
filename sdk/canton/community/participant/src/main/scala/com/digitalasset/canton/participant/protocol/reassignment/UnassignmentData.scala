// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree, Offset}
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{ReassignmentId, SerializableContract}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

/** Stores the data of an unassignment that needs to be passed from the source synchronizer to the
  * target synchronizer.
  */
final case class UnassignmentData(
    reassignmentId: ReassignmentId,
    unassignmentRequest: FullUnassignmentTree,
    unassignmentDecisionTime: CantonTimestamp,
    unassignmentResult: Option[DeliveredUnassignmentResult],
) {
  def contract: SerializableContract = unassignmentRequest.contract

  require(
    contract.contractId == unassignmentRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${unassignmentRequest.contractId} of the unassignment request.",
  )

  def targetSynchronizer: Target[SynchronizerId] = unassignmentRequest.targetSynchronizer

  def sourceSynchronizer: Source[SynchronizerId] = unassignmentRequest.sourceSynchronizer

  def unassignmentTs: CantonTimestamp = reassignmentId.unassignmentTs

  def sourceMediator: MediatorGroupRecipient = unassignmentRequest.mediator

  def reassignmentCounter: ReassignmentCounter = unassignmentRequest.reassignmentCounter

  def addUnassignmentResult(result: DeliveredUnassignmentResult): Option[UnassignmentData] =
    mergeUnassignmentResult(Some(result))

  def mergeWith(other: UnassignmentData): Option[UnassignmentData] =
    if (this eq other) Some(this)
    else
      other match {
        case UnassignmentData(
              `reassignmentId`,
              `unassignmentRequest`,
              `unassignmentDecisionTime`,
              otherResult,
            ) =>
          mergeUnassignmentResult(otherResult)
        case _ => None
      }

  private[this] def mergeUnassignmentResult(
      result: Option[DeliveredUnassignmentResult]
  ): Option[UnassignmentData] = {
    val oldResult = this.unassignmentResult
    OptionUtil
      .mergeEqual(oldResult, result)
      .map(merged => if (merged eq oldResult) this else this.copy(unassignmentResult = merged))
  }
}

object UnassignmentData {
  sealed trait ReassignmentGlobalOffset extends Product with Serializable {
    def merge(other: ReassignmentGlobalOffset): Either[String, ReassignmentGlobalOffset]

    def unassignment: Option[Offset]
    def assignment: Option[Offset]
  }

  object ReassignmentGlobalOffset {
    def create(
        unassignment: Option[Offset],
        assignment: Option[Offset],
    ): Either[String, Option[ReassignmentGlobalOffset]] =
      (unassignment, assignment) match {
        case (Some(unassignment), Some(assignment)) =>
          ReassignmentGlobalOffsets.create(unassignment, assignment).map(Some(_))
        case (Some(unassignment), None) => Right(Some(UnassignmentGlobalOffset(unassignment)))
        case (None, Some(assignment)) => Right(Some(AssignmentGlobalOffset(assignment)))
        case (None, None) => Right(None)
      }
  }

  final case class UnassignmentGlobalOffset(offset: Offset) extends ReassignmentGlobalOffset {
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

    override def unassignment: Option[Offset] = Some(offset)
    override def assignment: Option[Offset] = None
  }

  final case class AssignmentGlobalOffset(offset: Offset) extends ReassignmentGlobalOffset {
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

    override def unassignment: Option[Offset] = None
    override def assignment: Option[Offset] = Some(offset)
  }

  final case class ReassignmentGlobalOffsets private (
      unassignmentOffset: Offset,
      assignmentOffset: Offset,
  ) extends ReassignmentGlobalOffset {
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

    override def unassignment: Option[Offset] = Some(unassignmentOffset)
    override def assignment: Option[Offset] = Some(assignmentOffset)
  }

  object ReassignmentGlobalOffsets {
    def create(
        unassignment: Offset,
        assignment: Offset,
    ): Either[String, ReassignmentGlobalOffsets] =
      Either.cond(
        unassignment != assignment,
        ReassignmentGlobalOffsets(unassignment, assignment),
        s"Unassignment and assignment offsets should be different but got $unassignment",
      )
  }
}
