// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{GeneratorsCrypto, Salt, TestHash}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResultMessage,
  DeliveredTransferOutResult,
  SignedProtocolMessage,
  Verdict,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  GeneratorsProtocol as GeneratorsProtocolSequencing,
  MediatorsOfDomain,
  SignedContent,
  TimeProof,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.ExecutionContext

final class GeneratorsTransferData(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
    generatorsProtocolSequencing: GeneratorsProtocolSequencing,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import org.scalatest.EitherValues.*
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

  implicit val transferSubmitterMetadataArb: Arbitrary[TransferSubmitterMetadata] =
    Arbitrary(
      for {
        submitter <- Arbitrary.arbitrary[LfPartyId]
        applicationId <- applicationIdArb.arbitrary.map(_.unwrap)
        submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
        commandId <- commandIdArb.arbitrary.map(_.unwrap)
        submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
        workflowId <- Gen.option(workflowIdArb.arbitrary.map(_.unwrap))

      } yield TransferSubmitterMetadata(
        submitter,
        submittingParticipant,
        commandId,
        submissionId,
        applicationId,
        workflowId,
      )
    )

  implicit val transferInCommonData: Arbitrary[TransferInCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      targetDomain <- Arbitrary.arbitrary[TargetDomainId]

      targetMediator <- Arbitrary.arbitrary[MediatorsOfDomain]

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid

      submitterMetadata <- Arbitrary.arbitrary[TransferSubmitterMetadata]

      hashOps = TestHash // Not used for serialization

    } yield TransferInCommonData
      .create(hashOps)(
        salt,
        targetDomain,
        targetMediator,
        stakeholders,
        uuid,
        submitterMetadata,
        targetProtocolVersion,
      )
  )

  implicit val transferOutCommonData: Arbitrary[TransferOutCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      sourceDomain <- Arbitrary.arbitrary[SourceDomainId]

      sourceMediator <- Arbitrary.arbitrary[MediatorsOfDomain]

      stakeholders <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      adminParties <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      uuid <- Gen.uuid

      submitterMetadata <- Arbitrary.arbitrary[TransferSubmitterMetadata]

      hashOps = TestHash // Not used for serialization

    } yield TransferOutCommonData
      .create(hashOps)(
        salt,
        sourceDomain,
        sourceMediator,
        stakeholders,
        adminParties,
        uuid,
        submitterMetadata,
        sourceProtocolVersion,
      )
  )

  private def deliveryTransferOutResultGen(
      contract: SerializableContract,
      sourceProtocolVersion: SourceProtocolVersion,
  ): Gen[DeliveredTransferOutResult] =
    for {
      sourceDomain <- Arbitrary.arbitrary[SourceDomainId]
      requestId <- Arbitrary.arbitrary[RequestId]
      rootHash <- Arbitrary.arbitrary[RootHash]
      protocolVersion = sourceProtocolVersion.v
      verdict = Verdict.Approve(protocolVersion)

      result = ConfirmationResultMessage.create(
        sourceDomain.id,
        ViewType.TransferOutViewType,
        requestId,
        rootHash,
        verdict,
        contract.metadata.stakeholders,
        protocolVersion,
      )

      signedResult =
        SignedProtocolMessage.from(
          result,
          protocolVersion,
          GeneratorsCrypto.sign(
            "TransferOutResult-mediator",
            TestHash.testHashPurpose,
          ),
        )

      recipients <- recipientsArb.arbitrary

      transferOutTimestamp <- Arbitrary.arbitrary[CantonTimestamp]

      batch = Batch.of(protocolVersion, signedResult -> recipients)
      deliver <- deliverGen(sourceDomain.unwrap, batch)
    } yield DeliveredTransferOutResult {
      SignedContent(
        deliver,
        sign("TransferOutResult-sequencer", TestHash.testHashPurpose),
        Some(transferOutTimestamp),
        protocolVersion,
      )
    }

  implicit val transferInViewArb: Arbitrary[TransferInView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
      creatingTransactionId <- Arbitrary.arbitrary[TransactionId]
      transferOutResultEvent <- deliveryTransferOutResultGen(contract, sourceProtocolVersion)
      transferCounter <- transferCounterGen

      hashOps = TestHash // Not used for serialization

    } yield TransferInView
      .create(hashOps)(
        salt,
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

      creatingTransactionId <- Arbitrary.arbitrary[TransactionId]
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary

      targetDomain <- Arbitrary.arbitrary[TargetDomainId]
      timeProof <- Arbitrary.arbitrary[TimeProof]
      transferCounter <- transferCounterGen

      hashOps = TestHash // Not used for serialization

    } yield TransferOutView
      .create(hashOps)(
        salt,
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
