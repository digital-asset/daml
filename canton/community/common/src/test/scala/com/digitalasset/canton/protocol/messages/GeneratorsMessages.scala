// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{GeneratorsCrypto, Signature}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  CantonTimestampSecond,
  GeneratorsData,
  GeneratorsDataTime,
  ViewPosition,
  ViewType,
}
import com.digitalasset.canton.protocol.{GeneratorsProtocol, RequestId, RootHash, TransferDomainId}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.GeneratorsTransaction
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

final class GeneratorsMessages(
    protocolVersion: ProtocolVersion,
    generatorsData: GeneratorsData,
    generatorsDataTime: GeneratorsDataTime,
    generatorsProtocol: GeneratorsProtocol,
    generatorsTransaction: GeneratorsTransaction,
    generatorsLocalVerdict: GeneratorsLocalVerdict,
    generatorsVerdict: GeneratorsVerdict,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsData.*
  import generatorsDataTime.*
  import generatorsProtocol.*
  import generatorsTransaction.*
  import generatorsLocalVerdict.*
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

  implicit val malformedMediatorRequestResultArb: Arbitrary[MalformedMediatorRequestResult] =
    Arbitrary(
      for {
        requestId <- Arbitrary.arbitrary[RequestId]
        domainId <- Arbitrary.arbitrary[DomainId]
        viewType <- Arbitrary.arbitrary[ViewType]
        mediatorReject <- mediatorRejectArb.arbitrary
      } yield MalformedMediatorRequestResult.tryCreate(
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

  implicit val mediatorResponseArb: Arbitrary[MediatorResponse] = Arbitrary(
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

    } yield MediatorResponse.tryCreate(
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
  implicit val mediatorResultArb: Arbitrary[MediatorResult] = Arbitrary(
    Gen.oneOf[MediatorResult](
      Arbitrary.arbitrary[MalformedMediatorRequestResult],
      Arbitrary.arbitrary[TransactionResultMessage],
      Arbitrary.arbitrary[TransferResult[TransferDomainId]],
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val signedProtocolMessageContentArb: Arbitrary[SignedProtocolMessageContent] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[AcsCommitment],
      Arbitrary.arbitrary[MalformedMediatorRequestResult],
      Arbitrary.arbitrary[MediatorResponse],
      Arbitrary.arbitrary[MediatorResult],
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

  private implicit val emptyTraceContext: TraceContext = TraceContext.empty
  private lazy val syncCrypto = GeneratorsCrypto.cryptoFactory.headSnapshot

  implicit val domainTopologyTransactionMessageArb: Arbitrary[DomainTopologyTransactionMessage] =
    Arbitrary(
      for {
        transactions <- Gen.listOf(
          signedTopologyTransactionArb.arbitrary
        )
        domainId <- Arbitrary.arbitrary[DomainId]
        notSequencedAfter <- Arbitrary.arbitrary[CantonTimestamp]
      } yield Await.result(
        DomainTopologyTransactionMessage.tryCreate(
          transactions,
          syncCrypto,
          domainId,
          notSequencedAfter,
          protocolVersion,
        ),
        10.seconds,
      )
    )

  // TODO(#14241) Once we have more generators for merkle trees base classes, make these generators exhaustive
  implicit val unsignedProtocolMessageArb: Arbitrary[UnsignedProtocolMessage] =
    Arbitrary(domainTopologyTransactionMessageArb.arbitrary)

  // TODO(#14515) Check that the generator is exhaustive
  implicit val protocolMessageArb: Arbitrary[ProtocolMessage] =
    Arbitrary(unsignedProtocolMessageArb.arbitrary)

  // TODO(#14515) Check that the generator is exhaustive
  implicit val envelopeContentArb: Arbitrary[EnvelopeContent] = Arbitrary(for {
    protocolMessage <- protocolMessageArb.arbitrary
  } yield EnvelopeContent.tryCreate(protocolMessage, protocolVersion))

}
