// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.participant.state.ReassignmentCommand.{Assign, Unassign}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

sealed trait ReassignmentCommandsBatch

object ReassignmentCommandsBatch {

  final case class Unassignments(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
      contractIds: NonEmpty[Seq[LfContractId]],
  ) extends ReassignmentCommandsBatch

  final case class Assignments(target: Target[SynchronizerId], reassignmentId: ReassignmentId)
      extends ReassignmentCommandsBatch

  sealed trait InvalidBatch
  case object NoCommands extends InvalidBatch
  case object MixedAssignmentWithOtherCommands extends InvalidBatch
  case object UnassignmentsWithDifferingSynchronizers extends InvalidBatch

  def create(commands: Seq[ReassignmentCommand]): Either[InvalidBatch, ReassignmentCommandsBatch] =
    commands match {
      case Nil => Left(NoCommands)
      case Seq(assign: Assign) =>
        Right(
          Assignments(
            target = assign.targetSynchronizer,
            reassignmentId = ReassignmentId(assign.sourceSynchronizer, assign.unassignId),
          )
        )
      case (head: Unassign) +: tail =>
        validateUnassigns(
          Unassignments(
            source = head.sourceSynchronizer,
            target = head.targetSynchronizer,
            contractIds = NonEmpty.mk(Seq, head.contractId),
          ),
          tail,
        )
      case _ => Left(MixedAssignmentWithOtherCommands)
    }

  private def validateUnassigns(
      soFar: Unassignments,
      rest: Seq[ReassignmentCommand],
  ): Either[InvalidBatch, Unassignments] = rest match {
    case Nil => Right(soFar.copy(contractIds = reverse1(soFar.contractIds)))
    case (head: Unassign) +: tail =>
      if (head.sourceSynchronizer == soFar.source && head.targetSynchronizer == soFar.target)
        validateUnassigns(soFar.copy(contractIds = head.contractId +: soFar.contractIds), tail)
      else
        Left(UnassignmentsWithDifferingSynchronizers)
    case _ => Left(MixedAssignmentWithOtherCommands)
  }

  // Reversing a sequence does not change its cardinality, so is safe on a NonEmpty.
  // However, there isn't currently an appropriate method on the NonEmpty type.
  private def reverse1[T](items: NonEmpty[Seq[T]]): NonEmpty[Seq[T]] =
    NonEmpty.from(items.toSeq.reverse).getOrElse(???)
}
