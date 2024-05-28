// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.GeneratorsConfig
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  Encrypted,
  SecureRandomness,
  Signature,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  CantonTimestampSecond,
  FullInformeeTree,
  GeneratorsData,
  TransferInViewTree,
  TransferOutViewTree,
  ViewPosition,
  ViewType,
}
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast.Broadcast
import com.digitalasset.canton.protocol.{GeneratorsProtocol, RequestId, RootHash, ViewHash}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.GeneratorsTransaction
import com.digitalasset.canton.topology.{DomainId, GeneratorsTopology, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{Generators, LfPartyId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.ExecutionContext

final class GeneratorsMessages(
    protocolVersion: ProtocolVersion,
    generatorsData: GeneratorsData,
    generatorsProtocol: GeneratorsProtocol,
    generatorsLocalVerdict: GeneratorsLocalVerdict,
    generatorsVerdict: GeneratorsVerdict,
    generatorTransactions: GeneratorsTransaction,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsData.*
  import generatorsLocalVerdict.*
  import generatorsProtocol.*
  import generatorsVerdict.*
  import generatorTransactions.*

  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  /*
   Execution context is needed for crypto operations. Since wiring a proper ec would be
   too complex here, using the global one.
   */
  private implicit val ec: ExecutionContext = ExecutionContext.global

  implicit val acsCommitmentArb: Arbitrary[AcsCommitment] = Arbitrary(
    for {
      domainId <- Arbitrary.arbitrary[DomainId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      counterParticipant <- Arbitrary.arbitrary[ParticipantId]

      periodFrom <- Arbitrary.arbitrary[CantonTimestampSecond]
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = CommitmentPeriod(periodFrom, periodDuration)

      commitment <- byteStringArb.arbitrary
    } yield AcsCommitment.create(
      domainId,
      sender,
      counterParticipant,
      period,
      commitment,
      protocolVersion,
    )
  )

  implicit val confirmationResultMessageArb: Arbitrary[ConfirmationResultMessage] = Arbitrary(
    for {
      domainId <- Arbitrary.arbitrary[DomainId]
      viewType <- Arbitrary.arbitrary[ViewType]
      requestId <- Arbitrary.arbitrary[RequestId]
      rootHash <- Arbitrary.arbitrary[RootHash]
      verdict <- verdictArb.arbitrary
      informees <- Arbitrary.arbitrary[Set[LfPartyId]]

      // TODO(#14515) Also generate instance that makes pv above cover all the values
    } yield ConfirmationResultMessage.create(
      domainId,
      viewType,
      requestId,
      rootHash,
      verdict,
      informees,
      protocolVersion,
    )
  )

  implicit val confirmationResponseArb: Arbitrary[ConfirmationResponse] = Arbitrary(
    for {
      requestId <- Arbitrary.arbitrary[RequestId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      localVerdict <- localVerdictArb.arbitrary

      domainId <- Arbitrary.arbitrary[DomainId]

      confirmingParties <-
        if (localVerdict.isMalformed) Gen.const(Set.empty[LfPartyId])
        else nonEmptySet(implicitly[Arbitrary[LfPartyId]]).arbitrary.map(_.forgetNE)

      rootHash <- Arbitrary.arbitrary[RootHash]

      viewPositionO <- localVerdict match {
        case _: LocalApprove | _: LocalReject =>
          Gen.some(Arbitrary.arbitrary[ViewPosition])
        case _ => Gen.option(Arbitrary.arbitrary[ViewPosition])
      }

    } yield ConfirmationResponse.tryCreate(
      requestId,
      sender,
      viewPositionO,
      localVerdict,
      rootHash,
      confirmingParties,
      domainId,
      protocolVersion,
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val signedProtocolMessageContentArb: Arbitrary[SignedProtocolMessageContent] = Arbitrary(
    Gen.oneOf[SignedProtocolMessageContent](
      Arbitrary.arbitrary[AcsCommitment],
      Arbitrary.arbitrary[ConfirmationResponse],
      Arbitrary.arbitrary[ConfirmationResultMessage],
    )
  )

  implicit val typedSignedProtocolMessageContent
      : Arbitrary[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]] = Arbitrary(for {
    content <- Arbitrary.arbitrary[SignedProtocolMessageContent]
  } yield TypedSignedProtocolMessageContent(content, protocolVersion))

  implicit val signedProtocolMessageArb
      : Arbitrary[SignedProtocolMessage[SignedProtocolMessageContent]] = Arbitrary(
    for {
      typedMessage <- Arbitrary
        .arbitrary[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]]

      signatures <- nonEmptyListGen(implicitly[Arbitrary[Signature]])
    } yield SignedProtocolMessage(typedMessage, signatures)(
      SignedProtocolMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  implicit val serializedRootHashMessagePayloadArb: Arbitrary[SerializedRootHashMessagePayload] =
    Arbitrary(
      for {
        bytes <- byteStringArb.arbitrary
      } yield SerializedRootHashMessagePayload(bytes)
    )

  implicit val rootHashMessagePayloadArb: Arbitrary[RootHashMessagePayload] = Arbitrary(
    // Gen.oneOf(
    Arbitrary.arbitrary[SerializedRootHashMessagePayload]
    // TODO(#17020): Disabled EmptyRootHashMessagePayload for now - figure out how to properly compare objects
    //  e.g using: EmptyRootHashMessagePayload.emptyRootHashMessagePayloadCast
    //  , Gen.const[RootHashMessagePayload](EmptyRootHashMessagePayload)
    // )
  )

  val informeeMessageArb: Arbitrary[InformeeMessage] = Arbitrary(
    for {
      fullInformeeTree <- Arbitrary.arbitrary[FullInformeeTree]
      submittingParticipantSignature <- Arbitrary.arbitrary[Signature]
    } yield InformeeMessage(fullInformeeTree, submittingParticipantSignature)(protocolVersion)
  )

  implicit val asymmetricEncrypted: Arbitrary[AsymmetricEncrypted[SecureRandomness]] = Arbitrary(
    for {
      encrypted <- byteStringArb.arbitrary
      fingerprint <- GeneratorsTopology.fingerprintArb.arbitrary
    } yield AsymmetricEncrypted(encrypted, fingerprint)
  )

  val encryptedViewMessage: Arbitrary[EncryptedViewMessage[ViewType]] = Arbitrary(
    for {
      signatureO <- Gen.option(Arbitrary.arbitrary[Signature])
      viewHash <- Arbitrary.arbitrary[ViewHash]
      encryptedViewBytestring <- byteStringArb.arbitrary
      randomness = Encrypted.fromByteString[SecureRandomness](encryptedViewBytestring)
      sessionKey <- Generators.nonEmptyListGen[AsymmetricEncrypted[SecureRandomness]]
      viewType <- viewTypeArb.arbitrary
      encryptedView = EncryptedView(viewType)(Encrypted.fromByteString(encryptedViewBytestring))
      domainId <- Arbitrary.arbitrary[DomainId]
      viewEncryptionScheme <- genArbitrary[SymmetricKeyScheme].arbitrary
    } yield EncryptedViewMessage.apply(
      submittingParticipantSignature = signatureO,
      viewHash = viewHash,
      randomness = randomness,
      sessionKey = sessionKey,
      encryptedView = encryptedView,
      domainId = domainId,
      viewEncryptionScheme = viewEncryptionScheme,
      protocolVersion = protocolVersion,
    )
  )

  private val transferInMediatorMessageArb: Arbitrary[TransferInMediatorMessage] = Arbitrary(
    for {
      tree <- Arbitrary.arbitrary[TransferInViewTree]
      submittingParticipantSignature <- Arbitrary.arbitrary[Signature]
    } yield TransferInMediatorMessage(tree, submittingParticipantSignature)
  )

  private val transferOutMediatorMessageArb: Arbitrary[TransferOutMediatorMessage] = Arbitrary(
    for {
      tree <- Arbitrary.arbitrary[TransferOutViewTree]
      submittingParticipantSignature <- Arbitrary.arbitrary[Signature]
    } yield TransferOutMediatorMessage(tree, submittingParticipantSignature)
  )

  implicit val rootHashMessageArb: Arbitrary[RootHashMessage[RootHashMessagePayload]] =
    Arbitrary(
      for {
        rootHash <- Arbitrary.arbitrary[RootHash]
        domainId <- Arbitrary.arbitrary[DomainId]
        viewType <- viewTypeArb.arbitrary
        submissionTopologyTime <- Arbitrary.arbitrary[CantonTimestamp]
        payload <- Arbitrary.arbitrary[RootHashMessagePayload]
      } yield RootHashMessage.apply(
        rootHash,
        domainId,
        protocolVersion,
        viewType,
        submissionTopologyTime,
        payload,
      )
    )

  implicit val broadcast: Arbitrary[Broadcast] = Arbitrary(
    for {
      id <- GeneratorsConfig.string255Arb.arbitrary
      transactions <- Gen.listOfN(10, signedTopologyTransactionArb.arbitrary)
    } yield Broadcast(id, transactions)
  )

  implicit val topologyTransactionsBroadcast: Arbitrary[TopologyTransactionsBroadcast] = Arbitrary(
    for {
      domainId <- Arbitrary.arbitrary[DomainId]
      broadcast <- broadcast.arbitrary
    } yield TopologyTransactionsBroadcast.create(domainId, Seq(broadcast), protocolVersion)
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val unsignedProtocolMessageArb: Arbitrary[UnsignedProtocolMessage] =
    Arbitrary(
      Gen.oneOf(
        rootHashMessageArb.arbitrary,
        informeeMessageArb.arbitrary,
        encryptedViewMessage.arbitrary,
        transferInMediatorMessageArb.arbitrary,
        transferOutMediatorMessageArb.arbitrary,
        topologyTransactionsBroadcast.arbitrary,
      )
    )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val protocolMessageArb: Arbitrary[ProtocolMessage] =
    Arbitrary(unsignedProtocolMessageArb.arbitrary)

  // TODO(#14515) Check that the generator is exhaustive
  implicit val envelopeContentArb: Arbitrary[EnvelopeContent] = Arbitrary(for {
    protocolMessage <- protocolMessageArb.arbitrary
  } yield EnvelopeContent.tryCreate(protocolMessage, protocolVersion))
}
