// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{GeneratorsCrypto, HashPurpose, Salt, TestHash}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  SignedProtocolMessage,
  TransferResult,
  Verdict,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  GeneratorsProtocol as GeneratorsProtocolSequencing,
  SignedContent,
  TimeProof,
}
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.ExecutionContext

final class GeneratorsTransferData(
    protocolVersion: ProtocolVersion,
    generatorsDataTime: GeneratorsDataTime,
    generatorsProtocol: GeneratorsProtocol,
    generatorsProtocolSequencing: GeneratorsProtocolSequencing,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import org.scalatest.EitherValues.*
  import generatorsDataTime.*
  import generatorsProtocol.*
  import generatorsProtocolSequencing.*

  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  /*
   Execution context is needed for crypto operations. Since wiring a proper ec would be
   too complex here, using the global one.
   */
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val sourceProtocolVersion = SourceProtocolVersion(protocolVersion)
  private val targetProtocolVersion = TargetProtocolVersion(protocolVersion)

  implicit val transferInCommonData: Arbitrary[TransferInCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      targetDomain <- Arbitrary.arbitrary[TargetDomainId]

      targetMediator <- Arbitrary.arbitrary[MediatorRef]
      singleTargetMediator <- Arbitrary.arbitrary[MediatorRef.Single]

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid

      updatedTargetMediator =
        if (!TransferCommonData.isGroupMediatorSupported(targetProtocolVersion.v))
          singleTargetMediator
        else targetMediator

      hashOps = TestHash // Not used for serialization

    } yield TransferInCommonData
      .create(hashOps)(
        salt,
        targetDomain,
        updatedTargetMediator,
        stakeholders,
        uuid,
        targetProtocolVersion,
      )
      .value
  )

  implicit val transferOutCommonData: Arbitrary[TransferOutCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      sourceDomain <- Arbitrary.arbitrary[SourceDomainId]

      sourceMediator <- Arbitrary.arbitrary[MediatorRef]
      singleSourceMediator <- Arbitrary.arbitrary[MediatorRef.Single]

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      adminParties <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid

      updatedSourceMediator =
        if (!TransferCommonData.isGroupMediatorSupported(sourceProtocolVersion.v))
          singleSourceMediator
        else sourceMediator

      hashOps = TestHash // Not used for serialization

    } yield TransferOutCommonData
      .create(hashOps)(
        salt,
        sourceDomain,
        updatedSourceMediator,
        stakeholders,
        adminParties,
        uuid,
        sourceProtocolVersion,
      )
      .value
  )

  private def transferInSubmitterMetadataGen(
      targetProtocolVersion: TargetProtocolVersion
  ): Gen[TransferSubmitterMetadata] =
    for {
      submitter <- Arbitrary.arbitrary[LfPartyId]
      applicationId <- defaultValueGen(
        targetProtocolVersion.v,
        TransferInView.applicationIdDefaultValue,
        applicationIdArb.arbitrary.map(_.unwrap),
      )
      submittingParticipant <- defaultValueGen(
        targetProtocolVersion.v,
        TransferInView.submittingParticipantDefaultValue,
        Arbitrary.arbitrary[ParticipantId].map(_.toLf),
      )
      commandId <- defaultValueGen(
        targetProtocolVersion.v,
        TransferInView.commandIdDefaultValue,
        commandIdArb.arbitrary.map(_.unwrap),
      )
      submissionId <- defaultValueGen(
        targetProtocolVersion.v,
        TransferInView.submissionIdDefaultValue,
        Gen.option(ledgerSubmissionIdArb.arbitrary),
      )
      workflowId <- defaultValueGen(
        targetProtocolVersion.v,
        TransferInView.workflowIdDefaultValue,
        Gen.option(workflowIdArb.arbitrary.map(_.unwrap)),
      )
    } yield TransferSubmitterMetadata(
      submitter,
      applicationId,
      submittingParticipant,
      commandId,
      submissionId,
      workflowId,
    )

  private def transferOutSubmitterMetadataGen(
      sourceProtocolVersion: SourceProtocolVersion
  ): Gen[TransferSubmitterMetadata] =
    for {
      submitter <- Arbitrary.arbitrary[LfPartyId]
      applicationId <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.applicationIdDefaultValue,
        applicationIdArb.arbitrary.map(_.unwrap),
      )
      submittingParticipant <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.submittingParticipantDefaultValue,
        Arbitrary.arbitrary[ParticipantId].map(_.toLf),
      )
      commandId <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.commandIdDefaultValue,
        commandIdArb.arbitrary.map(_.unwrap),
      )
      submissionId <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.submissionIdDefaultValue,
        Gen.option(ledgerSubmissionIdArb.arbitrary),
      )
      workflowId <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.workflowIdDefaultValue,
        Gen.option(workflowIdArb.arbitrary.map(_.unwrap)),
      )
    } yield TransferSubmitterMetadata(
      submitter,
      applicationId,
      submittingParticipant,
      commandId,
      submissionId,
      workflowId,
    )

  private def deliveryTransferOutResultGen(
      contract: SerializableContract,
      sourceProtocolVersion: SourceProtocolVersion,
  ): Gen[DeliveredTransferOutResult] =
    for {
      sourceDomain <- Arbitrary.arbitrary[SourceDomainId]

      requestId <- Arbitrary.arbitrary[RequestId]
      protocolVersion = sourceProtocolVersion.v

      verdict = Verdict.Approve(protocolVersion)
      result = TransferResult.create(
        requestId,
        contract.metadata.stakeholders,
        sourceDomain,
        verdict,
        protocolVersion,
      )

      signedResult =
        SignedProtocolMessage.tryFrom(
          result,
          protocolVersion,
          GeneratorsCrypto.sign("TransferOutResult-mediator", HashPurpose.TransferResultSignature),
        )

      recipients <- recipientsArb.arbitrary

      transferOutTimestamp <- Arbitrary.arbitrary[CantonTimestamp]

      batch = Batch.of(protocolVersion, signedResult -> recipients)
      deliver <- deliverGen(sourceDomain.unwrap, batch)
    } yield DeliveredTransferOutResult {
      SignedContent(
        deliver,
        sign("TransferOutResult-sequencer", HashPurpose.TransferResultSignature),
        Some(transferOutTimestamp),
        protocolVersion,
      )
    }

  implicit val transferInViewArb: Arbitrary[TransferInView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      contract <- serializableContractArb.arbitrary
      creatingTransactionId <- Arbitrary.arbitrary[TransactionId]
      submitterMetadata <- transferInSubmitterMetadataGen(targetProtocolVersion)
      transferOutResultEvent <- deliveryTransferOutResultGen(contract, sourceProtocolVersion)
      transferCounter <- transferCounterOGen(targetProtocolVersion.v)

      hashOps = TestHash // Not used for serialization

    } yield TransferInView
      .create(hashOps)(
        salt,
        submitterMetadata,
        contract,
        creatingTransactionId,
        transferOutResultEvent,
        sourceProtocolVersion,
        targetProtocolVersion,
        transferCounter,
      )
      .value
  )

  implicit val transferOutViewArb: Arbitrary[TransferOutView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]

      submitterMetadata <- transferOutSubmitterMetadataGen(sourceProtocolVersion)
      creatingTransactionId <- Arbitrary.arbitrary[TransactionId]
      contract <- generatorsProtocol.serializableContractArb.arbitrary

      targetDomain <- Arbitrary.arbitrary[TargetDomainId]
      timeProof <- Arbitrary.arbitrary[TimeProof]
      transferCounter <- transferCounterOGen(sourceProtocolVersion.v)

      hashOps = TestHash // Not used for serialization

    } yield TransferOutView
      .create(hashOps)(
        salt,
        submitterMetadata,
        creatingTransactionId,
        contract,
        targetDomain,
        timeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
        transferCounter,
      )
  )

}
