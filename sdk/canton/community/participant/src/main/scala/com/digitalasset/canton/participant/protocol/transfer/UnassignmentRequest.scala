// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.CanSubmitTransfer
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, TimeProof}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Request to transfer a contract away from a domain.
  *
  * @param adminParties admin parties of participants that (a) host a stakeholder of the contract and
  *                     (b) are connected to both source and target domain
  * @param targetTimeProof a sequenced event that the submitter has recently observed on the target domain.
  *                        Determines the timestamp of the topology at the target domain.
  * @param reassignmentCounter The new reassignment counter (incremented value compared to the one in the ACS).
  */
final case class UnassignmentRequest(
    submitterMetadata: TransferSubmitterMetadata,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
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
        adminParties,
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
      submitterMetadata: TransferSubmitterMetadata,
      stakeholders: Set[LfPartyId],
      sourceDomain: SourceDomainId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceMediator: MediatorGroupRecipient,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
      reassignmentCounter: ReassignmentCounter,
      logger: TracedLogger,
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
      _ <- CanSubmitTransfer.unassignment(
        contractId,
        sourceTopology,
        submitterMetadata.submitter,
        participantId,
      )
      adminPartiesAndRecipients <- AdminPartiesAndParticipants(
        contractId,
        submitterMetadata.submitter,
        stakeholders,
        sourceTopology,
        targetTopology,
        logger,
      )
      _ <- TransferKnownAndVetted(
        stakeholders,
        targetTopology,
        contractId,
        templateId.packageId,
        targetDomain,
      )
    } yield {
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata,
        stakeholders,
        adminPartiesAndRecipients.adminParties,
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

      UnassignmentRequestValidated(unassignmentRequest, adminPartiesAndRecipients.participants)
    }
  }

}
