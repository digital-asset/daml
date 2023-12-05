// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.crypto.{GeneratorsCrypto, HashPurpose, Salt, TestHash}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  SignedProtocolMessage,
  TransferResult,
  Verdict,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, SignedContent}
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  ApplicationId,
  LedgerCommandId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.ExecutionContext

object GeneratorsTransferData {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import org.scalatest.EitherValues.*

  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  /*
   Execution context is needed for crypto operations. Since wiring a proper ec would be
   too complex here, using the global one.
   */
  private implicit val ec: ExecutionContext = ExecutionContext.global

  implicit val transferInCommonData: Arbitrary[TransferInCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      targetDomain <- Arbitrary.arbitrary[TargetDomainId]

      targetMediator <- Arbitrary.arbitrary[MediatorRef]

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid
      targetProtocolVersion <- Arbitrary.arbitrary[TargetProtocolVersion]

      hashOps = TestHash // Not used for serialization

    } yield TransferInCommonData
      .create(hashOps)(
        salt,
        targetDomain,
        targetMediator,
        stakeholders,
        uuid,
        targetProtocolVersion,
      )
  )

  implicit val transferOutCommonData: Arbitrary[TransferOutCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      sourceDomain <- Arbitrary.arbitrary[SourceDomainId]

      sourceMediator <- Arbitrary.arbitrary[MediatorRef]

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      adminParties <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid
      sourceProtocolVersion <- Arbitrary.arbitrary[SourceProtocolVersion]

      hashOps = TestHash // Not used for serialization

    } yield TransferOutCommonData
      .create(hashOps)(
        salt,
        sourceDomain,
        sourceMediator,
        stakeholders,
        adminParties,
        uuid,
        sourceProtocolVersion,
      )
  )

  private def transferInSubmitterMetadataGen(
      protocolVersion: TargetProtocolVersion
  ): Gen[TransferSubmitterMetadata] =
    for {
      submitter <- Arbitrary.arbitrary[LfPartyId]
      applicationId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.applicationIdDefaultValue,
        applicationIdArb.arbitrary.map(_.unwrap),
      )
      submittingParticipant <- defaultValueGen(
        protocolVersion.v,
        TransferInView.submittingParticipantDefaultValue,
        Arbitrary.arbitrary[ParticipantId].map(_.toLf),
      )
      commandId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.commandIdDefaultValue,
        commandIdArb.arbitrary.map(_.unwrap),
      )
      submissionId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.submissionIdDefaultValue,
        Gen.option(ledgerSubmissionIdArb.arbitrary),
      )
      workflowId <- defaultValueGen(
        protocolVersion.v,
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

  implicit val transferOutSubmitterMetadataArb: Arbitrary[TransferSubmitterMetadata] =
    Arbitrary(
      for {
        submitter <- Arbitrary.arbitrary[LfPartyId]
        applicationId <- Arbitrary.arbitrary[ApplicationId]
        submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
        commandId <- Arbitrary.arbitrary[LedgerCommandId]
        submissionId <- Gen.option(Arbitrary.arbitrary[LedgerSubmissionId])
        workflowId <- Gen.option(Arbitrary.arbitrary[LfWorkflowId])
      } yield TransferSubmitterMetadata(
        submitter,
        applicationId.unwrap,
        submittingParticipant.toLf,
        commandId,
        submissionId,
        workflowId,
      )
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
        SignedProtocolMessage.from(
          result,
          protocolVersion,
          GeneratorsCrypto.sign("TransferOutResult-mediator", HashPurpose.TransferResultSignature),
        )

      recipients <- recipientsArb(protocolVersion).arbitrary

      transferOutTimestamp <- Arbitrary.arbitrary[CantonTimestamp]

      batch = Batch.of(protocolVersion, signedResult -> recipients)
      deliver <- deliverGen(sourceDomain.unwrap, batch, protocolVersion)
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

      targetProtocolVersion <- Arbitrary.arbitrary[TargetProtocolVersion]
      sourceProtocolVersion <- Arbitrary.arbitrary[SourceProtocolVersion]

      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
      creatingTransactionId <- Arbitrary.arbitrary[TransactionId]
      submitterMetadata <- transferInSubmitterMetadataGen(targetProtocolVersion)
      transferOutResultEvent <- deliveryTransferOutResultGen(contract, sourceProtocolVersion)
      transferCounter <- transferCounterOGen

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

      sourceProtocolVersion <- Arbitrary.arbitrary[SourceProtocolVersion]
      targetProtocolVersion <- Arbitrary.arbitrary[TargetProtocolVersion]

      submitterMetadata <- Arbitrary.arbitrary[TransferSubmitterMetadata]

      creatingTransactionId <- Arbitrary.arbitrary[TransactionId]
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary

      targetDomain <- Arbitrary.arbitrary[TargetDomainId]
      timeProof <- Arbitrary.arbitrary[TimeProof]
      transferCounter <- transferCounterOGen

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
