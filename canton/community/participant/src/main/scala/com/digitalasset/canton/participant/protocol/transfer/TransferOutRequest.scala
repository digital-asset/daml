// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.CanSubmitTransfer
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, TransferCounterO}

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Request to transfer a contract away from a domain.
  *
  * @param adminParties admin parties of participants that (a) host a stakeholder of the contract and
  *                     (b) are connected to both source and target domain
  * @param targetTimeProof a sequenced event that the submitter has recently observed on the target domain.
  *                        Determines the timestamp of the topology at the target domain.
  * @param transferCounter The new transfer counter (incremented value compared to the one in the ACS).
  */
final case class TransferOutRequest(
    submitterMetadata: TransferSubmitterMetadata,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    creatingTransactionId: TransactionId,
    contract: SerializableContract,
    sourceDomain: SourceDomainId,
    sourceProtocolVersion: SourceProtocolVersion,
    sourceMediator: MediatorRef,
    targetDomain: TargetDomainId,
    targetProtocolVersion: TargetProtocolVersion,
    targetTimeProof: TimeProof,
    transferCounter: TransferCounterO,
) {

  def toFullTransferOutTree(
      hashOps: HashOps,
      hmacOps: HmacOps,
      seed: SaltSeed,
      uuid: UUID,
  ): FullTransferOutTree = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, hmacOps)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, hmacOps)

    val commonData = TransferOutCommonData
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

    val view = TransferOutView
      .create(hashOps)(
        viewSalt,
        creatingTransactionId,
        contract,
        targetDomain,
        targetTimeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
        transferCounter,
      )

    FullTransferOutTree(TransferOutViewTree(commonData, view, sourceProtocolVersion.v, hashOps))
  }
}

object TransferOutRequest {

  def validated(
      participantId: ParticipantId,
      timeProof: TimeProof,
      creatingTransactionId: TransactionId,
      contract: SerializableContract,
      submitterMetadata: TransferSubmitterMetadata,
      stakeholders: Set[LfPartyId],
      sourceDomain: SourceDomainId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceMediator: MediatorRef,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
      transferCounter: TransferCounterO,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    TransferOutRequestValidated,
  ] = {
    val contractId = contract.contractId
    val templateId = contract.contractInstance.unversioned.template

    for {
      _ <- CanSubmitTransfer.transferOut(
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
      val transferOutRequest = TransferOutRequest(
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
        transferCounter,
      )

      TransferOutRequestValidated(transferOutRequest, adminPartiesAndRecipients.participants)
    }
  }

}
