// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  Encrypted,
  SecureRandomness,
  Signature,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.data.{
  AssignmentViewTree,
  CantonTimestamp,
  CantonTimestampSecond,
  FullInformeeTree,
  GeneratorsData,
  UnassignmentViewTree,
  ViewPosition,
  ViewType,
}
import com.digitalasset.canton.protocol.{GeneratorsProtocol, RequestId, RootHash, ViewHash}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.{
  GeneratorsTransaction,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{GeneratorsTopology, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{Generators, LfPartyId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

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

  implicit val acsCommitmentArb: Arbitrary[AcsCommitment] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      counterParticipant <- Arbitrary.arbitrary[ParticipantId]

      periodFrom <- Arbitrary.arbitrary[CantonTimestampSecond]
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = CommitmentPeriod(periodFrom, periodDuration)

      commitment <- byteStringArb.arbitrary
    } yield AcsCommitment.create(
      synchronizerId,
      sender,
      counterParticipant,
      period,
      commitment,
      protocolVersion,
    )
  )

  implicit val confirmationResultMessageArb: Arbitrary[ConfirmationResultMessage] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      viewType <- Arbitrary.arbitrary[ViewType]
      requestId <- Arbitrary.arbitrary[RequestId]
      rootHash <- Arbitrary.arbitrary[RootHash]
      verdict <- verdictArb.arbitrary

      // TODO(#14515) Also generate instance that makes pv above cover all the values
    } yield ConfirmationResultMessage.create(
      synchronizerId,
      viewType,
      requestId,
      rootHash,
      verdict,
      protocolVersion,
    )
  )

  implicit val confirmationResponseArb: Arbitrary[ConfirmationResponse] = Arbitrary(
    for {
      localVerdict <- localVerdictArb.arbitrary
      confirmingParties <-
        if (localVerdict.isMalformed) Gen.const(Set.empty[LfPartyId])
        else nonEmptySet(implicitly[Arbitrary[LfPartyId]]).arbitrary.map(_.forgetNE)
      viewPositionO <- localVerdict match {
        case _: LocalApprove | _: LocalReject =>
          Gen.some(Arbitrary.arbitrary[ViewPosition])
        case _ => Gen.option(Arbitrary.arbitrary[ViewPosition])
      }
    } yield ConfirmationResponse.tryCreate(
      viewPositionO,
      localVerdict,
      confirmingParties,
    )
  )

  implicit val confirmationResponsesArb: Arbitrary[ConfirmationResponses] = Arbitrary(
    for {
      requestId <- Arbitrary.arbitrary[RequestId]
      rootHash <- Arbitrary.arbitrary[RootHash]
      synchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      responses <- Gen.nonEmptyListOf(confirmationResponseArb.arbitrary)
      responsesNE = NonEmptyUtil.fromUnsafe(responses)
      confirmationResponses = ConfirmationResponses.tryCreate(
        requestId,
        rootHash,
        synchronizerId,
        sender,
        responsesNE,
        protocolVersion,
      )
    } yield confirmationResponses
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val signedProtocolMessageContentArb: Arbitrary[SignedProtocolMessageContent] = Arbitrary(
    Gen.oneOf[SignedProtocolMessageContent](
      Arbitrary.arbitrary[AcsCommitment],
      Arbitrary.arbitrary[ConfirmationResponses],
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
      encryptionAlgorithmSpec <- encryptionAlgorithmSpecArb.arbitrary
      fingerprint <- GeneratorsTopology.fingerprintArb.arbitrary
    } yield AsymmetricEncrypted(encrypted, encryptionAlgorithmSpec, fingerprint)
  )

  val encryptedViewMessage: Arbitrary[EncryptedViewMessage[ViewType]] = Arbitrary(
    for {
      signatureO <- Gen.option(Arbitrary.arbitrary[Signature])
      viewHash <- Arbitrary.arbitrary[ViewHash]
      encryptedViewBytestring <- byteStringArb.arbitrary
      sessionKey <- Generators.nonEmptyListGen[AsymmetricEncrypted[SecureRandomness]]
      viewType <- viewTypeArb.arbitrary
      encryptedView = EncryptedView(viewType)(Encrypted.fromByteString(encryptedViewBytestring))
      synchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      viewEncryptionScheme <- genArbitrary[SymmetricKeyScheme].arbitrary
    } yield EncryptedViewMessage.apply(
      submittingParticipantSignature = signatureO,
      viewHash = viewHash,
      sessionKeys = sessionKey,
      encryptedView = encryptedView,
      synchronizerId = synchronizerId,
      viewEncryptionScheme = viewEncryptionScheme,
      protocolVersion = protocolVersion,
    )
  )

  private val assignmentMediatorMessageArb: Arbitrary[AssignmentMediatorMessage] = Arbitrary(
    for {
      tree <- Arbitrary.arbitrary[AssignmentViewTree]
      submittingParticipantSignature <- Arbitrary.arbitrary[Signature]
    } yield AssignmentMediatorMessage(tree, submittingParticipantSignature)(
      AssignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  private val unassignmentMediatorMessageArb: Arbitrary[UnassignmentMediatorMessage] = Arbitrary(
    for {
      tree <- Arbitrary.arbitrary[UnassignmentViewTree]
      submittingParticipantSignature <- Arbitrary.arbitrary[Signature]
      rpv = UnassignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion)
    } yield UnassignmentMediatorMessage(tree, submittingParticipantSignature)(rpv)
  )

  implicit val rootHashMessageArb: Arbitrary[RootHashMessage[RootHashMessagePayload]] =
    Arbitrary(
      for {
        rootHash <- Arbitrary.arbitrary[RootHash]
        synchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
        viewType <- viewTypeArb.arbitrary
        submissionTopologyTime <- Arbitrary.arbitrary[CantonTimestamp]
        payload <- Arbitrary.arbitrary[RootHashMessagePayload]
      } yield RootHashMessage.apply(
        rootHash,
        synchronizerId,
        protocolVersion,
        viewType,
        submissionTopologyTime,
        payload,
      )
    )

  implicit val topologyTransactionsBroadcast: Arbitrary[TopologyTransactionsBroadcast] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      transactions <- Gen.listOf(
        Arbitrary.arbitrary[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]]
      )
    } yield TopologyTransactionsBroadcast(synchronizerId, transactions, protocolVersion)
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val unsignedProtocolMessageArb: Arbitrary[UnsignedProtocolMessage] =
    Arbitrary(
      Gen.oneOf[UnsignedProtocolMessage](
        rootHashMessageArb.arbitrary,
        informeeMessageArb.arbitrary,
        encryptedViewMessage.arbitrary,
        assignmentMediatorMessageArb.arbitrary,
        unassignmentMediatorMessageArb.arbitrary,
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
