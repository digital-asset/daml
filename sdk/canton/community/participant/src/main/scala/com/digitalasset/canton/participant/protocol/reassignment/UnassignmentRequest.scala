// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, TimeProof}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Request to reassign a contract away from a synchronizer.
  *
  * @param reassigningParticipants
  *   The list of reassigning participants
  * @param targetTimeProof
  *   a sequenced event that the submitter has recently observed on the target synchronizer.
  *   Determines the timestamp of the topology at the target synchronizer.
  * @param reassignmentCounter
  *   The new reassignment counter (incremented value compared to the one in the ACS).
  */
final case class UnassignmentRequest(
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassigningParticipants: Set[ParticipantId],
    contracts: ContractsReassignmentBatch,
    sourceSynchronizer: Source[SynchronizerId],
    sourceProtocolVersion: Source[ProtocolVersion],
    sourceMediator: MediatorGroupRecipient,
    targetSynchronizer: Target[SynchronizerId],
    targetProtocolVersion: Target[ProtocolVersion],
    targetTimeProof: TimeProof,
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
        sourceSynchronizer,
        sourceMediator,
        stakeholders = contracts.stakeholders,
        reassigningParticipants,
        uuid = uuid,
        submitterMetadata,
        sourceProtocolVersion,
      )

    val view = UnassignmentView
      .create(hashOps)(
        viewSalt,
        contracts,
        targetSynchronizer,
        targetTimeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
      )

    FullUnassignmentTree(UnassignmentViewTree(commonData, view, sourceProtocolVersion, hashOps))
  }
}

object UnassignmentRequest {

  def validated(
      participantId: ParticipantId,
      timeProof: TimeProof,
      contracts: ContractsReassignmentBatch,
      submitterMetadata: ReassignmentSubmitterMetadata,
      sourceSynchronizer: Source[SynchronizerId],
      sourceProtocolVersion: Source[ProtocolVersion],
      sourceMediator: MediatorGroupRecipient,
      targetSynchronizer: Target[SynchronizerId],
      targetProtocolVersion: Target[ProtocolVersion],
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Target[TopologySnapshot],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentValidationError,
    UnassignmentRequestValidated,
  ] = {
    val contractIds = contracts.contractIds.toSet
    val packageIds = contracts.contracts.view.map(_.templateId.packageId).toSet
    val stakeholders = contracts.stakeholders

    for {
      _ <- ReassignmentValidation
        .checkSubmitter(
          ReassignmentRef.ContractIdRef(contractIds),
          sourceTopology,
          submitterMetadata.submitter,
          participantId,
          stakeholders = stakeholders.all,
        )

      unassignmentRequestRecipients <- sourceTopology.unwrap
        .activeParticipantsOfAll(stakeholders.all.toList)
        .leftMap(inactiveParties =>
          ReassignmentValidationError.StakeholderHostingErrors(
            s"The following stakeholders are not active: $inactiveParties"
          )
        )

      reassigningParticipants <- new ReassigningParticipantsComputation(
        stakeholders = stakeholders,
        sourceTopology,
        targetTopology,
      ).compute

      _ <- UsableSynchronizers
        .checkPackagesVetted(
          targetSynchronizer.unwrap,
          targetTopology.unwrap,
          stakeholders.all.view.map(_ -> packageIds).toMap,
          targetTopology.unwrap.referenceTime,
        )
        .leftMap[ReassignmentValidationError](unknownPackage =>
          PackageIdUnknownOrUnvetted(contractIds, unknownPackage.unknownTo)
        )
    } yield {
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata,
        reassigningParticipants,
        contracts,
        sourceSynchronizer,
        sourceProtocolVersion,
        sourceMediator,
        targetSynchronizer,
        targetProtocolVersion,
        timeProof,
      )

      UnassignmentRequestValidated(
        unassignmentRequest,
        unassignmentRequestRecipients,
      )
    }
  }

}
