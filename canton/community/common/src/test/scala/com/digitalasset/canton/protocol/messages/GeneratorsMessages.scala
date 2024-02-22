// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{CantonTimestampSecond, GeneratorsData, ViewPosition, ViewType}
import com.digitalasset.canton.protocol.{GeneratorsProtocol, RequestId, RootHash, TransferDomainId}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.ExecutionContext

final class GeneratorsMessages(
    protocolVersion: ProtocolVersion,
    generatorsData: GeneratorsData,
    generatorsProtocol: GeneratorsProtocol,
    generatorsLocalVerdict: GeneratorsLocalVerdict,
    generatorsVerdict: GeneratorsVerdict,
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

  implicit val transferResultArb: Arbitrary[TransferResult[TransferDomainId]] = Arbitrary(for {
    requestId <- Arbitrary.arbitrary[RequestId]
    informees <- Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
    domain <- Arbitrary.arbitrary[TransferDomainId]
    verdict <- verdictArb.arbitrary
  } yield TransferResult.create(requestId, informees, domain, verdict, protocolVersion))

  implicit val MalformedMediatorConfirmationRequestResultArb
      : Arbitrary[MalformedConfirmationRequestResult] =
    Arbitrary(
      for {
        requestId <- Arbitrary.arbitrary[RequestId]
        domainId <- Arbitrary.arbitrary[DomainId]
        viewType <- Arbitrary.arbitrary[ViewType]
        mediatorReject <- mediatorRejectArb.arbitrary
      } yield MalformedConfirmationRequestResult.tryCreate(
        requestId,
        domainId,
        viewType,
        mediatorReject,
        protocolVersion,
      )
    )

  implicit val transactionResultMessageArb: Arbitrary[TransactionResultMessage] = Arbitrary(for {
    verdict <- verdictArb.arbitrary
    rootHash <- Arbitrary.arbitrary[RootHash]
    requestId <- Arbitrary.arbitrary[RequestId]
    domainId <- Arbitrary.arbitrary[DomainId]

    // TODO(#14241) Also generate instance that contains InformeeTree + make pv above cover all the values
  } yield TransactionResultMessage(requestId, verdict, rootHash, domainId, protocolVersion))

  implicit val confirmationResponseArb: Arbitrary[ConfirmationResponse] = Arbitrary(
    for {
      requestId <- Arbitrary.arbitrary[RequestId]
      sender <- Arbitrary.arbitrary[ParticipantId]
      localVerdict <- localVerdictArb.arbitrary

      domainId <- Arbitrary.arbitrary[DomainId]

      confirmingParties <- localVerdict match {
        case _: Malformed =>
          Gen.const(Set.empty[LfPartyId])
        case _: LocalApprove | _: LocalReject =>
          nonEmptySet(implicitly[Arbitrary[LfPartyId]]).arbitrary.map(_.forgetNE)
        case _ => Gen.containerOf[Set, LfPartyId](Arbitrary.arbitrary[LfPartyId])
      }

      rootHash <- localVerdict match {
        case _: LocalApprove | _: LocalReject => Gen.some(Arbitrary.arbitrary[RootHash])
        case _ => Gen.option(Arbitrary.arbitrary[RootHash])
      }

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
  implicit val mediatorResultArb: Arbitrary[ConfirmationResult] = Arbitrary(
    Gen.oneOf[ConfirmationResult](
      Arbitrary.arbitrary[MalformedConfirmationRequestResult],
      Arbitrary.arbitrary[TransactionResultMessage],
      Arbitrary.arbitrary[TransferResult[TransferDomainId]],
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val signedProtocolMessageContentArb: Arbitrary[SignedProtocolMessageContent] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[AcsCommitment],
      Arbitrary.arbitrary[MalformedConfirmationRequestResult],
      Arbitrary.arbitrary[ConfirmationResponse],
      Arbitrary.arbitrary[ConfirmationResult],
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

  implicit val rootHashMessageArb: Arbitrary[RootHashMessage[RootHashMessagePayload]] =
    Arbitrary(
      for {
        rootHash <- Arbitrary.arbitrary[RootHash]
        domainId <- Arbitrary.arbitrary[DomainId]
        viewType <- Arbitrary.arbitrary[ViewType]
        payload <- Arbitrary.arbitrary[RootHashMessagePayload]
      } yield RootHashMessage.apply(
        rootHash,
        domainId,
        protocolVersion,
        viewType,
        payload,
      )
    )

  // TODO(#14241) Once we have more generators for merkle trees base classes, make these generators exhaustive
  implicit val unsignedProtocolMessageArb: Arbitrary[UnsignedProtocolMessage] =
    Arbitrary(rootHashMessageArb.arbitrary)

  // TODO(#14515) Check that the generator is exhaustive
  implicit val protocolMessageArb: Arbitrary[ProtocolMessage] =
    Arbitrary(unsignedProtocolMessageArb.arbitrary)

  // TODO(#14515) Check that the generator is exhaustive
  implicit val envelopeContentArb: Arbitrary[EnvelopeContent] = Arbitrary(for {
    protocolMessage <- protocolMessageArb.arbitrary
  } yield EnvelopeContent.tryCreate(protocolMessage, protocolVersion))

}
