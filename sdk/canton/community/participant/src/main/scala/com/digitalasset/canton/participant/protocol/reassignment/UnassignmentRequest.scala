// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.CanSubmitReassignment
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.UnassignmentSubmitterMustBeStakeholder
import com.digitalasset.canton.participant.protocol.submission.UsableDomain
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, TimeProof}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.Reassignment.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Request to reassign a contract away from a domain.
  *
  * @param reassigningParticipants The list of reassigning participants
  * @param targetTimeProof a sequenced event that the submitter has recently observed on the target domain.
  *                        Determines the timestamp of the topology at the target domain.
  * @param reassignmentCounter The new reassignment counter (incremented value compared to the one in the ACS).
  */
final case class UnassignmentRequest(
    submitterMetadata: ReassignmentSubmitterMetadata,
    stakeholders: Set[LfPartyId],
    reassigningParticipants: Set[ParticipantId],
    creatingTransactionId: TransactionId,
    contract: SerializableContract,
    sourceDomain: SourceDomainId,
    sourceProtocolVersion: SourceProtocolVersion,
    sourceMediator: MediatorGroupRecipient,
    targetDomain: TargetDomainId,
    targetProtocolVersion: TargetProtocolVersion,
    targetTimeProof: TimeProof,
    reassignmentCounter: ReassignmentCounter,
) {

  def toFullUnassignmentTree(
      hashOps: HashOps,
      hmacOps: HmacOps,
      seed: SaltSeed,
      uuid: UUID,
  ): FullUnassignmentTree = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, hmacOps)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, hmacOps)

    val commonData = UnassignmentCommonData
      .create(hashOps)(
        commonDataSalt,
        sourceDomain,
        sourceMediator,
        stakeholders,
        reassigningParticipants,
        uuid,
        submitterMetadata,
        sourceProtocolVersion,
      )

    val view = UnassignmentView
      .create(hashOps)(
        viewSalt,
        contract,
        creatingTransactionId,
        targetDomain,
        targetTimeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
        reassignmentCounter,
      )

    FullUnassignmentTree(UnassignmentViewTree(commonData, view, sourceProtocolVersion, hashOps))
  }
}

object UnassignmentRequest {

  def validated(
      participantId: ParticipantId,
      timeProof: TimeProof,
      creatingTransactionId: TransactionId,
      contract: SerializableContract,
      submitterMetadata: ReassignmentSubmitterMetadata,
      stakeholders: Set[LfPartyId],
      sourceDomain: SourceDomainId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceMediator: MediatorGroupRecipient,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
      reassignmentCounter: ReassignmentCounter,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    UnassignmentRequestValidated,
  ] = {
    val contractId = contract.contractId
    val templateId = contract.contractInstance.unversioned.template

    for {
      _ <- CanSubmitReassignment.unassignment(
        contractId,
        sourceTopology,
        submitterMetadata.submitter,
        participantId,
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        stakeholders.contains(submitterMetadata.submitter),
        UnassignmentSubmitterMustBeStakeholder(
          contractId,
          submitterMetadata.submitter,
          stakeholders,
        ),
      )
      adminPartiesAndRecipients <- AdminPartiesAndParticipants(
        stakeholders,
        sourceTopology,
        targetTopology,
      )
      reassigningParticipants <- ReassigningParticipants
        .compute(
          stakeholders,
          sourceTopology,
          targetTopology,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- UsableDomain
        .checkPackagesVetted(
          targetDomain.unwrap,
          targetTopology,
          stakeholders.view.map(_ -> Set(templateId.packageId)).toMap,
          targetTopology.referenceTime,
        )
        .leftMap[ReassignmentProcessorError](unknownPackage =>
          UnassignmentProcessorError
            .PackageIdUnknownOrUnvetted(contractId, unknownPackage.unknownTo)
        )
    } yield {
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata,
        stakeholders,
        reassigningParticipants,
        creatingTransactionId,
        contract,
        sourceDomain,
        sourceProtocolVersion,
        sourceMediator,
        targetDomain,
        targetProtocolVersion,
        timeProof,
        reassignmentCounter,
      )

      UnassignmentRequestValidated(
        unassignmentRequest,
        adminPartiesAndRecipients.unassigningParticipants,
      )
    }
  }

}
