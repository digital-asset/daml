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
import com.digitalasset.canton.topology.MediatorRef
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
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
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

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid

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

  private implicit val transferSubmitterMetadata: Arbitrary[TransferSubmitterMetadata] =
    Arbitrary(
      for {
        submitter <- Arbitrary.arbitrary[LfPartyId]
      } yield TransferSubmitterMetadata(
        submitter = submitter,
        applicationId = TransferInView.applicationIdDefaultValue,
        submittingParticipant = TransferInView.submittingParticipantDefaultValue,
        commandId = TransferInView.commandIdDefaultValue,
        submissionId = TransferInView.submissionIdDefaultValue,
        workflowId = TransferInView.workflowIdDefaultValue,
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
      submitterMetadata <- Arbitrary.arbitrary[TransferSubmitterMetadata]
      transferOutResultEvent <- deliveryTransferOutResultGen(contract, sourceProtocolVersion)

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
      )
      .value
  )

  implicit val transferOutViewArb: Arbitrary[TransferOutView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]

      submitterMetadata <- Arbitrary.arbitrary[TransferSubmitterMetadata]
      contract <- generatorsProtocol.serializableContractArb.arbitrary

      targetDomain <- Arbitrary.arbitrary[TargetDomainId]
      timeProof <- Arbitrary.arbitrary[TimeProof]

      hashOps = TestHash // Not used for serialization

    } yield TransferOutView
      .create(hashOps)(
        salt,
        submitterMetadata,
        contract,
        targetDomain,
        timeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
      )
  )

}
