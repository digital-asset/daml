// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

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
  GeneratorsTrafficData,
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
import com.digitalasset.canton.{Generators, GeneratorsLf, LfPartyId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsMessages(
    protocolVersion: ProtocolVersion,
    generatorsData: GeneratorsData,
    generatorsLf: GeneratorsLf,
    generatorsProtocol: GeneratorsProtocol,
    generatorsLocalVerdict: GeneratorsLocalVerdict,
    generatorsVerdict: GeneratorsVerdict,
    generatorsTopology: GeneratorsTopology,
    generatorTransactions: GeneratorsTransaction,
    generatorsTrafficData: GeneratorsTrafficData,
) {
  import com.digitalasset.canton.Generators.*
  import generatorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import generatorsTopology.*
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
      psid <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      viewType <- Arbitrary.arbitrary[ViewType]
      requestId <- Arbitrary.arbitrary[RequestId]
      rootHash <- Arbitrary.arbitrary[RootHash]
      verdict <- verdictArb.arbitrary
    } yield ConfirmationResultMessage.create(
      psid,
      viewType,
      requestId,
      rootHash,
      verdict,
    )
  )

  implicit val confirmationResponseArb: Arbitrary[ConfirmationResponse] = Arbitrary(
    for {
      localVerdict <- localVerdictArb.arbitrary
      confirmingParties <-
        if (localVerdict.isMalformed) Gen.const(Set.empty[LfPartyId])
        else nonEmptySet(implicitly[Arbitrary[LfPartyId]]).arbitrary.map(_.forgetNE)
      viewPositionO <- localVerdict match {
        case _: LocalApprove | _: LocalReject | _: LocalAbstain =>
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
      responsesNE <- nonEmptyListGen[ConfirmationResponse]
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

  implicit val signedProtocolMessageContentArb: Arbitrary[SignedProtocolMessageContent] =
    arbitraryForAllSubclasses(classOf[SignedProtocolMessageContent])(
      GeneratorForClass(Arbitrary.arbitrary[AcsCommitment], classOf[AcsCommitment]),
      GeneratorForClass(Arbitrary.arbitrary[ConfirmationResponses], classOf[ConfirmationResponses]),
      GeneratorForClass(
        generatorsTrafficData.setTrafficPurchasedArb.arbitrary,
        classOf[SetTrafficPurchasedMessage],
      ),
      GeneratorForClass(
        Arbitrary.arbitrary[ConfirmationResultMessage],
        classOf[ConfirmationResultMessage],
      ),
    )

  implicit val typedSignedProtocolMessageContent
      : Arbitrary[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]] = Arbitrary(for {
    content <- Arbitrary.arbitrary[SignedProtocolMessageContent]
  } yield TypedSignedProtocolMessageContent(content))

  implicit val signedProtocolMessageArb
      : Arbitrary[SignedProtocolMessage[SignedProtocolMessageContent]] = Arbitrary(
    for {
      typedMessage <- Arbitrary
        .arbitrary[TypedSignedProtocolMessageContent[SignedProtocolMessageContent]]

      signatures <- nonEmptyListGen(implicitly[Arbitrary[Signature]])
    } yield SignedProtocolMessage(typedMessage, signatures)
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
      fingerprint <- generatorsTopology.fingerprintArb.arbitrary
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

  val assignmentMediatorMessageArb: Arbitrary[AssignmentMediatorMessage] = Arbitrary(
    for {
      tree <- Arbitrary.arbitrary[AssignmentViewTree]
      submittingParticipantSignature <- Arbitrary.arbitrary[Signature]
    } yield AssignmentMediatorMessage(tree, submittingParticipantSignature)(
      AssignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  val unassignmentMediatorMessageArb: Arbitrary[UnassignmentMediatorMessage] = Arbitrary(
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
        psid <- Arbitrary.arbitrary[PhysicalSynchronizerId]
        viewType <- viewTypeArb.arbitrary
        submissionTopologyTime <- Arbitrary.arbitrary[CantonTimestamp]
        payload <- Arbitrary.arbitrary[RootHashMessagePayload]
      } yield RootHashMessage.apply(
        rootHash,
        psid,
        viewType,
        submissionTopologyTime,
        payload,
      )
    )

  implicit val topologyTransactionsBroadcast: Arbitrary[TopologyTransactionsBroadcast] = Arbitrary(
    for {
      psid <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      transactions <- boundedListGen[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]]
    } yield TopologyTransactionsBroadcast(psid, transactions)
  )

  implicit val unsignedProtocolMessageArb: Arbitrary[UnsignedProtocolMessage] =
    arbitraryForAllSubclasses(classOf[UnsignedProtocolMessage])(
      GeneratorForClass(
        rootHashMessageArb.arbitrary,
        classOf[RootHashMessage[RootHashMessagePayload]],
      ),
      GeneratorForClass(informeeMessageArb.arbitrary, classOf[InformeeMessage]),
      GeneratorForClass(encryptedViewMessage.arbitrary, classOf[EncryptedViewMessage[ViewType]]),
      GeneratorForClass(assignmentMediatorMessageArb.arbitrary, classOf[AssignmentMediatorMessage]),
      GeneratorForClass(
        unassignmentMediatorMessageArb.arbitrary,
        classOf[UnassignmentMediatorMessage],
      ),
      GeneratorForClass(
        topologyTransactionsBroadcast.arbitrary,
        classOf[TopologyTransactionsBroadcast],
      ),
    )

  implicit val protocolMessageArb: Arbitrary[ProtocolMessage] =
    arbitraryForAllSubclasses(classOf[ProtocolMessage])(
      GeneratorForClass(unsignedProtocolMessageArb.arbitrary, classOf[UnsignedProtocolMessage]),
      GeneratorForClass(
        signedProtocolMessageArb.arbitrary,
        classOf[SignedProtocolMessage[SignedProtocolMessageContent]],
      ),
    )

  implicit val envelopeContentArb: Arbitrary[EnvelopeContent] = Arbitrary(for {
    unsignedProtocolMessage <- unsignedProtocolMessageArb.arbitrary
  } yield EnvelopeContent(unsignedProtocolMessage, protocolVersion))
}
