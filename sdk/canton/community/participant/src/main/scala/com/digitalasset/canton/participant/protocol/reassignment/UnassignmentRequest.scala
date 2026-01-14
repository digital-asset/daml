// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Request to reassign a contract away from a synchronizer.
  *
  * @param reassigningParticipants
  *   The list of reassigning participants
  * @param targetTimestamp
  *   The timestamp of the topology at the target synchronizer to be used for validation.
  */
final case class UnassignmentRequest(
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassigningParticipants: Set[ParticipantId],
    contracts: ContractsReassignmentBatch,
    sourceSynchronizer: Source[PhysicalSynchronizerId],
    sourceMediator: MediatorGroupRecipient,
    targetSynchronizer: Target[PhysicalSynchronizerId],
    targetTimestamp: Target[CantonTimestamp],
) {
  private val sourceProtocolVersion = sourceSynchronizer.map(_.protocolVersion)

  def mkReassignmentId(unassignmentTs: CantonTimestamp) = ReassignmentId(
    sourceSynchronizer.map(_.logical),
    targetSynchronizer.map(_.logical),
    unassignmentTs,
    contracts.contractIdCounters,
  )

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
        targetTimestamp,
        sourceProtocolVersion,
      )

    FullUnassignmentTree(UnassignmentViewTree(commonData, view, sourceProtocolVersion, hashOps))
  }
}

object UnassignmentRequest {

  def validated(
      participantId: ParticipantId,
      contracts: ContractsReassignmentBatch,
      contractValidator: ContractValidator,
      submitterMetadata: ReassignmentSubmitterMetadata,
      sourcePSId: Source[PhysicalSynchronizerId],
      sourceMediator: MediatorGroupRecipient,
      targetPSId: Target[PhysicalSynchronizerId],
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
          sourcePSId.unwrap,
          sourceTopology.unwrap,
          stakeholders.all.view.map(_ -> contracts.sourcePackageIds.unwrap).toMap,
          sourceTopology.unwrap.referenceTime,
        )
        .leftMap[ReassignmentValidationError](unknownPackage =>
          PackageIdUnknownOrUnvetted(contractIds, unknownPackage.unknownTo, sourcePSId.unwrap)
        )

      _ <- UsableSynchronizers
        .checkPackagesVetted(
          targetPSId.unwrap,
          targetTopology.unwrap,
          stakeholders.all.view.map(_ -> contracts.targetPackageIds.unwrap).toMap,
          targetTopology.unwrap.referenceTime,
        )
        .leftMap[ReassignmentValidationError](unknownPackage =>
          PackageIdUnknownOrUnvetted(contractIds, unknownPackage.unknownTo, targetPSId.unwrap)
        )

      _ <- ReassignmentValidation.authenticateContracts(
        contractValidator,
        contracts.contracts,
      )

    } yield {
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata,
        reassigningParticipants,
        contracts,
        sourcePSId,
        sourceMediator,
        targetPSId,
        targetTopology.map(_.timestamp),
      )

      UnassignmentRequestValidated(
        unassignmentRequest,
        unassignmentRequestRecipients,
      )
    }
  }

}
