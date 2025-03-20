// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.syntax.either.*
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree, Offset}
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData.ReassignmentEventGlobalOffset
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData.ReassignmentGlobalOffset
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.Source

/** Stores the data for a reassignment that is incomplete, i.e., for which only the assignment or
  * the unassignment was emitted to the indexer.
  *
  * If [[IncompleteReassignmentData.ReassignmentEventGlobalOffset]] is a
  * [[IncompleteReassignmentData.UnassignmentEventGlobalOffset]], it means that the unassignment
  * event was emitted before or at `queryOffset` and that assigned event was not yet emitted or at
  * an offset greater than `queryOffset`. The same holds symmetrically for a
  * [[IncompleteReassignmentData.AssignmentEventGlobalOffset]].
  */
final case class IncompleteReassignmentData(
    reassignmentId: ReassignmentId,
    reassigningParticipants: Option[Set[ParticipantId]],
    reassignmentEventGlobalOffset: ReassignmentEventGlobalOffset,
    queryOffset: Offset,
) {

  def unassignmentGlobalOffset: Option[Offset] =
    reassignmentEventGlobalOffset.unassignmentGlobalOffset
  def assignmentGlobalOffset: Option[Offset] = reassignmentEventGlobalOffset.assignmentGlobalOffset
}

object IncompleteReassignmentData {

  final case class InternalIncompleteReassignmentData(
      reassignmentId: ReassignmentId,
      unassignmentRequest: Option[FullUnassignmentTree],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      contract: SerializableContract,
  ) {
    def toIncompleteReassignmentData(
        queryOffset: Offset
    ): Either[String, IncompleteReassignmentData] =
      ReassignmentEventGlobalOffset
        .create(
          queryOffset = queryOffset,
          unassignmentGlobalOffset = reassignmentGlobalOffset.flatMap(_.unassignment),
          assignmentGlobalOffset = reassignmentGlobalOffset.flatMap(_.assignment),
        )
        .map(
          IncompleteReassignmentData(
            reassignmentId,
            unassignmentRequest.map(_.reassigningParticipants),
            _,
            queryOffset,
          )
        )
  }

  private def create(
      sourceSynchronizer: Source[SynchronizerId],
      unassignmentTs: CantonTimestamp,
      unassignmentRequest: Option[FullUnassignmentTree],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      queryOffset: Offset,
  ): Either[String, IncompleteReassignmentData] = {
    val reassignmentEventGlobalOffsetE: Either[String, ReassignmentEventGlobalOffset] =
      ReassignmentEventGlobalOffset.create(
        queryOffset = queryOffset,
        unassignmentGlobalOffset = reassignmentGlobalOffset.flatMap(_.unassignment),
        assignmentGlobalOffset = reassignmentGlobalOffset.flatMap(_.assignment),
      )

    reassignmentEventGlobalOffsetE.map { reassignmentEventGlobalOffset =>
      IncompleteReassignmentData(
        ReassignmentId(sourceSynchronizer, unassignmentTs),
        unassignmentRequest.map(_.reassigningParticipants),
        reassignmentEventGlobalOffset,
        queryOffset,
      )
    }
  }

  def tryCreate(
      sourceSynchronizer: Source[SynchronizerId],
      unassignmentTs: CantonTimestamp,
      unassignmentRequest: Option[FullUnassignmentTree],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      queryOffset: Offset,
  ): IncompleteReassignmentData =
    create(
      sourceSynchronizer,
      unassignmentTs,
      unassignmentRequest,
      reassignmentGlobalOffset,
      queryOffset,
    )
      .valueOr(err =>
        throw new IllegalArgumentException(s"Unable to create IncompleteReassignmentData: $err")
      )

  sealed trait ReassignmentEventGlobalOffset {
    def globalOffset: Offset
    def unassignmentGlobalOffset: Option[Offset]
    def assignmentGlobalOffset: Option[Offset]
  }

  final case class AssignmentEventGlobalOffset(globalOffset: Offset)
      extends ReassignmentEventGlobalOffset {
    override def unassignmentGlobalOffset: Option[Offset] = None

    override def assignmentGlobalOffset: Option[Offset] = Some(globalOffset)
  }

  final case class UnassignmentEventGlobalOffset(globalOffset: Offset)
      extends ReassignmentEventGlobalOffset {
    override def unassignmentGlobalOffset: Option[Offset] = Some(globalOffset)

    override def assignmentGlobalOffset: Option[Offset] = None
  }

  object ReassignmentEventGlobalOffset {
    private[reassignment] def create(
        queryOffset: Offset,
        unassignmentGlobalOffset: Option[Offset],
        assignmentGlobalOffset: Option[Offset],
    ): Either[String, ReassignmentEventGlobalOffset] =
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
            s"Expecting incomplete reassignment at offset $queryOffset, found unassignment=$unassignmentGlobalOffset and assignment=$assignmentGlobalOffset"
          )
      }
  }
}
