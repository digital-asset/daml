// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet.*
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  LfContractId,
  ReassignmentId,
  RequestId,
  SerializableContract,
  TargetDomainId,
}
import com.digitalasset.canton.util.SetsUtil.requireDisjoint
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

/** Describes the effect of a confirmation request on the active contracts, contract keys, and reassignments.
  * Transient contracts appear the following two sets:
  * <ol>
  *   <li>The union of [[creations]] and [[assignments]]</li>
  *   <li>The union of [[archivals]] or [[unassignments]]</li>
  * </ol>
  *
  * @param archivals    The contracts to be archived, along with their stakeholders. Must not contain contracts in [[unassignments]].
  * @param creations    The contracts to be created.
  * @param unassignments The contracts to be unassigned, along with their target domains and stakeholders.
  *                     Must not contain contracts in [[archivals]].
  * @param assignments  The contracts to be assigned, along with their reassignment IDs.
  * @throws java.lang.IllegalArgumentException if `unassignments` overlap with `archivals`
  *                                            or `creations` overlaps with `assignments`.
  */
final case class CommitSet(
    archivals: Map[LfContractId, ArchivalCommit],
    creations: Map[LfContractId, CreationCommit],
    unassignments: Map[LfContractId, UnassignmentCommit],
    assignments: Map[LfContractId, AssignmentCommit],
) extends PrettyPrinting {
  requireDisjoint(unassignments.keySet -> "unassignments", archivals.keySet -> "archivals")
  requireDisjoint(assignments.keySet -> "assignments", creations.keySet -> "creations")

  override def pretty: Pretty[CommitSet] = prettyOfClass(
    paramIfNonEmpty("archivals", _.archivals),
    paramIfNonEmpty("creations", _.creations),
    paramIfNonEmpty("unassignments", _.unassignments),
    paramIfNonEmpty("assigments", _.assignments),
  )
}

object CommitSet {

  val empty: CommitSet = CommitSet(Map.empty, Map.empty, Map.empty, Map.empty)

  final case class CreationCommit(
      contractMetadata: ContractMetadata,
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrinting {
    override def pretty: Pretty[CreationCommit] = prettyOfClass(
      param("contractMetadata", _.contractMetadata),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }
  final case class UnassignmentCommit(
      targetDomainId: TargetDomainId,
      stakeholders: Set[LfPartyId],
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrinting {
    override def pretty: Pretty[UnassignmentCommit] = prettyOfClass(
      param("targetDomainId", _.targetDomainId),
      paramIfNonEmpty("stakeholders", _.stakeholders),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }
  final case class AssignmentCommit(
      reassignmentId: ReassignmentId,
      contractMetadata: ContractMetadata,
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrinting {
    override def pretty: Pretty[AssignmentCommit] = prettyOfClass(
      param("reassignmentId", _.reassignmentId),
      param("contractMetadata", _.contractMetadata),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }
  final case class ArchivalCommit(
      stakeholders: Set[LfPartyId]
  ) extends PrettyPrinting {

    override def pretty: Pretty[ArchivalCommit] = prettyOfClass(
      param("stakeholders", _.stakeholders)
    )
  }

  def createForTransaction(
      activenessResult: ActivenessResult,
      requestId: RequestId,
      consumedInputsOfHostedParties: Map[LfContractId, Set[LfPartyId]],
      transient: Map[LfContractId, Set[LfPartyId]],
      createdContracts: Map[LfContractId, SerializableContract],
  )(implicit loggingContext: ErrorLoggingContext): CommitSet =
    if (activenessResult.isSuccessful) {
      val archivals = (consumedInputsOfHostedParties ++ transient).map {
        case (cid, hostedStakeholders) =>
          cid -> CommitSet.ArchivalCommit(hostedStakeholders)
      }
      val reassignmentCounter = ReassignmentCounter.Genesis
      val creations =
        createdContracts.fmap(c => CommitSet.CreationCommit(c.metadata, reassignmentCounter))
      CommitSet(
        archivals = archivals,
        creations = creations,
        unassignments = Map.empty,
        assignments = Map.empty,
      )
    } else {
      SyncServiceAlarm
        .Warn(s"Request $requestId with failed activeness check is approved.")
        .report()
      loggingContext.debug(s"Failed activeness result for request $requestId is $activenessResult")
      // TODO(i12904) Handle this case gracefully
      throw new RuntimeException(s"Request $requestId with failed activeness check is approved.")
    }
}
