// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.participant.protocol.submission.UsableDomains
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, TimeProof}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

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
    reassigningParticipants: Set[ParticipantId],
    contract: SerializableContract,
    sourceDomain: Source[DomainId],
    sourceProtocolVersion: Source[ProtocolVersion],
    sourceMediator: MediatorGroupRecipient,
    targetDomain: Target[DomainId],
    targetProtocolVersion: Target[ProtocolVersion],
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
        stakeholders = Stakeholders(contract.metadata),
        reassigningParticipants,
        uuid = uuid,
        submitterMetadata,
        sourceProtocolVersion,
      )

    val view = UnassignmentView
      .create(hashOps)(
        viewSalt,
        contract,
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
      contract: SerializableContract,
      submitterMetadata: ReassignmentSubmitterMetadata,
      sourceDomain: Source[DomainId],
      sourceProtocolVersion: Source[ProtocolVersion],
      sourceMediator: MediatorGroupRecipient,
      targetDomain: Target[DomainId],
      targetProtocolVersion: Target[ProtocolVersion],
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Target[TopologySnapshot],
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
    val stakeholders = Stakeholders(contract.metadata)

    for {
      _ <- ReassignmentValidation
        .checkSubmitter(
          ReassignmentRef(contractId),
          sourceTopology,
          submitterMetadata.submitter,
          participantId,
          stakeholders = stakeholders.all,
        )

      unassignmentRequestRecipients <- sourceTopology.unwrap
        .activeParticipantsOfAll(stakeholders.all.toList)
        .leftMap(inactiveParties =>
          StakeholderHostingErrors(s"The following stakeholders are not active: $inactiveParties")
        )

      reassigningParticipants <- new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology,
        targetTopology,
      ).compute

      _ <- UsableDomains
        .checkPackagesVetted(
          targetDomain.unwrap,
          targetTopology.unwrap,
          stakeholders.all.view.map(_ -> Set(templateId.packageId)).toMap,
          targetTopology.unwrap.referenceTime,
        )
        .leftMap[ReassignmentProcessorError](unknownPackage =>
          UnassignmentProcessorError
            .PackageIdUnknownOrUnvetted(contractId, unknownPackage.unknownTo)
        )
    } yield {
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata,
        reassigningParticipants,
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
        unassignmentRequestRecipients,
      )
    }
  }

}
