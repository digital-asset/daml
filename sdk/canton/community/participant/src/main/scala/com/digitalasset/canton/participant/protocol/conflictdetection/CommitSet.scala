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
import com.digitalasset.canton.{LfPartyId, TransferCounter}

/** Describes the effect of a confirmation request on the active contracts, contract keys, and transfers.
  * Transient contracts appear the following two sets:
  * <ol>
  *   <li>The union of [[creations]] and [[transferIns]]</li>
  *   <li>The union of [[archivals]] or [[transferOuts]]</li>
  * </ol>
  *
  * @param archivals The contracts to be archived, along with their stakeholders. Must not contain contracts in [[transferOuts]].
  * @param creations The contracts to be created.
  * @param transferOuts The contracts to be transferred out, along with their target domains and stakeholders.
  *                     Must not contain contracts in [[archivals]].
  * @param transferIns The contracts to be transferred in, along with their reassignment IDs.
  * @throws java.lang.IllegalArgumentException if `transferOuts` overlap with `archivals`
  *                                            or `creations` overlaps with `transferIns`.
  */
final case class CommitSet(
    archivals: Map[LfContractId, ArchivalCommit],
    creations: Map[LfContractId, CreationCommit],
    transferOuts: Map[LfContractId, TransferOutCommit],
    transferIns: Map[LfContractId, TransferInCommit],
) extends PrettyPrinting {
  requireDisjoint(transferOuts.keySet -> "Transfer-outs", archivals.keySet -> "archivals")
  requireDisjoint(transferIns.keySet -> "Transfer-ins", creations.keySet -> "creations")

  override def pretty: Pretty[CommitSet] = prettyOfClass(
    paramIfNonEmpty("archivals", _.archivals),
    paramIfNonEmpty("creations", _.creations),
    paramIfNonEmpty("transfer outs", _.transferOuts),
    paramIfNonEmpty("transfer ins", _.transferIns),
  )
}

object CommitSet {

  val empty: CommitSet = CommitSet(Map.empty, Map.empty, Map.empty, Map.empty)

  final case class CreationCommit(
      contractMetadata: ContractMetadata,
      transferCounter: TransferCounter,
  ) extends PrettyPrinting {
    override def pretty: Pretty[CreationCommit] = prettyOfClass(
      param("contractMetadata", _.contractMetadata),
      param("transferCounter", _.transferCounter),
    )
  }
  final case class TransferOutCommit(
      targetDomainId: TargetDomainId,
      stakeholders: Set[LfPartyId],
      transferCounter: TransferCounter,
  ) extends PrettyPrinting {
    override def pretty: Pretty[TransferOutCommit] = prettyOfClass(
      param("targetDomainId", _.targetDomainId),
      paramIfNonEmpty("stakeholders", _.stakeholders),
      param("transferCounter", _.transferCounter),
    )
  }
  final case class TransferInCommit(
      reassignmentId: ReassignmentId,
      contractMetadata: ContractMetadata,
      transferCounter: TransferCounter,
  ) extends PrettyPrinting {
    override def pretty: Pretty[TransferInCommit] = prettyOfClass(
      param("reassignmentId", _.reassignmentId),
      param("contractMetadata", _.contractMetadata),
      param("transferCounter", _.transferCounter),
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
      val transferCounter = TransferCounter.Genesis
      val creations =
        createdContracts.fmap(c => CommitSet.CreationCommit(c.metadata, transferCounter))
      CommitSet(
        archivals = archivals,
        creations = creations,
        transferOuts = Map.empty,
        transferIns = Map.empty,
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
