// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{CantonTimestamp, GeneratorsData}
import com.digitalasset.canton.protocol.TransactionAuthorizationPartySignatures
import com.digitalasset.canton.protocol.messages.{GeneratorsMessages, ProtocolMessage}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
}
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.{DomainId, Member, PartyId}
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.{GeneratorsVersion, ProtocolVersion}
import com.digitalasset.canton.{Generators, SequencerCounter}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsProtocol(
    protocolVersion: ProtocolVersion,
    generatorsMessages: GeneratorsMessages,
    generatorsData: GeneratorsData,
) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsMessages.*
  import GeneratorsProtocol.*

  implicit val acknowledgeRequestArb: Arbitrary[AcknowledgeRequest] = Arbitrary(for {
    ts <- Arbitrary.arbitrary[CantonTimestamp]
    member <- Arbitrary.arbitrary[Member]
  } yield AcknowledgeRequest(member, ts, protocolVersion))

  implicit val aggregationRuleArb: Arbitrary[AggregationRule] =
    Arbitrary(
      for {
        threshold <- Arbitrary.arbitrary[PositiveInt]
        eligibleMembers <- Generators.nonEmptyListGen[Member]
      } yield AggregationRule(eligibleMembers, threshold)(
        AggregationRule.protocolVersionRepresentativeFor(protocolVersion)
      )
    )

  implicit val closedEnvelopeArb: Arbitrary[ClosedEnvelope] = Arbitrary(for {
    bytes <- Arbitrary.arbitrary[ByteString]
    signatures <- Gen.listOfN(5, signatureArb.arbitrary)

    recipients <- recipientsArb.arbitrary
  } yield ClosedEnvelope.create(bytes, recipients, signatures, protocolVersion))

  implicit val openEnvelopArb: Arbitrary[OpenEnvelope[ProtocolMessage]] = Arbitrary(
    for {
      protocolMessage <- protocolMessageArb.arbitrary
      recipients <- recipientsArb.arbitrary
    } yield OpenEnvelope(protocolMessage, recipients)(protocolVersion)
  )

  implicit val envelopeArb: Arbitrary[Envelope[?]] =
    Arbitrary(Gen.oneOf[Envelope[?]](closedEnvelopeArb.arbitrary, openEnvelopArb.arbitrary))

  implicit val batchArb: Arbitrary[Batch[Envelope[?]]] =
    Arbitrary(for {
      envelopes <- Generators.nonEmptyListGen[Envelope[?]](envelopeArb)
    } yield Batch(envelopes.map(_.closeEnvelope), protocolVersion))

  implicit val submissionCostArb: Arbitrary[SequencingSubmissionCost] =
    Arbitrary(
      for {
        cost <- Arbitrary.arbitrary[NonNegativeLong]
      } yield SequencingSubmissionCost(cost)(
        SequencingSubmissionCost.protocolVersionRepresentativeFor(protocolVersion)
      )
    )

  implicit val submissionRequestArb: Arbitrary[SubmissionRequest] =
    Arbitrary(
      for {
        sender <- Arbitrary.arbitrary[Member]
        messageId <- Arbitrary.arbitrary[MessageId]
        envelopes <- Generators.nonEmptyListGen[ClosedEnvelope](closedEnvelopeArb)
        batch = Batch(envelopes.map(_.closeEnvelope), protocolVersion)
        maxSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
        aggregationRule <- Gen.option(Arbitrary.arbitrary[AggregationRule])
        submissionCost <- GeneratorsVersion.defaultValueGen(
          protocolVersion,
          SubmissionRequest.submissionCostDefaultValue,
          Gen.option(Arbitrary.arbitrary[SequencingSubmissionCost]),
        )
        topologyTimestamp <-
          if (aggregationRule.nonEmpty)
            Arbitrary.arbitrary[CantonTimestamp].map(Some(_))
          else Gen.const(None)
      } yield SubmissionRequest.tryCreate(
        sender,
        messageId,
        batch,
        maxSequencingTime,
        topologyTimestamp,
        aggregationRule,
        submissionCost,
        SubmissionRequest.protocolVersionRepresentativeFor(protocolVersion).representative,
      )
    )

  implicit val partySignaturesArb: Arbitrary[TransactionAuthorizationPartySignatures] = Arbitrary(
    for {
      signatures <- Arbitrary.arbitrary[Map[PartyId, Seq[Signature]]]
    } yield TransactionAuthorizationPartySignatures(signatures, protocolVersion)
  )

  implicit val topologyStateForInitRequestArb: Arbitrary[TopologyStateForInitRequest] = Arbitrary(
    for {
      member <- Arbitrary.arbitrary[Member]
    } yield TopologyStateForInitRequest(member, protocolVersion)
  )

  implicit val subscriptionRequestArb: Arbitrary[SubscriptionRequest] = Arbitrary(
    for {
      member <- Arbitrary.arbitrary[Member]
      counter <- Arbitrary.arbitrary[SequencerCounter]
    } yield SubscriptionRequest.apply(member, counter, protocolVersion)
  )

  private val sequencerDeliverErrorCodeArb: Arbitrary[SequencerDeliverErrorCode] = genArbitrary

  private implicit val sequencerDeliverErrorArb: Arbitrary[SequencerDeliverError] =
    Arbitrary(
      for {
        code <- sequencerDeliverErrorCodeArb.arbitrary
        message <- Arbitrary.arbitrary[String]
      } yield code.apply(message)
    )

  private implicit val deliverErrorArb: Arbitrary[DeliverError] = Arbitrary(
    for {
      sequencerCounter <- Arbitrary.arbitrary[SequencerCounter]
      ts <- Arbitrary.arbitrary[CantonTimestamp]
      domainId <- Arbitrary.arbitrary[DomainId]
      messageId <- Arbitrary.arbitrary[MessageId]
      error <- sequencerDeliverErrorArb.arbitrary
    } yield DeliverError.create(
      sequencerCounter,
      timestamp = ts,
      domainId = domainId,
      messageId,
      error,
      protocolVersion,
      Option.empty[TrafficReceipt],
    )
  )
  private implicit val deliverArbitrary: Arbitrary[Deliver[Envelope[?]]] = Arbitrary(
    for {
      domainId <- Arbitrary.arbitrary[DomainId]
      batch <- batchArb.arbitrary
      deliver <- Arbitrary(deliverGen(domainId, batch, protocolVersion)).arbitrary
    } yield deliver
  )

  implicit val sequencerEventArb: Arbitrary[SequencedEvent[Envelope[?]]] =
    Arbitrary(Gen.oneOf(deliverErrorArb.arbitrary, deliverArbitrary.arbitrary))

  implicit val signedContent: Arbitrary[SignedContent[HasCryptographicEvidence]] = Arbitrary(
    for {
      signatures <- Generators.nonEmptyListGen[Signature]
      ts <- Gen.option(Arbitrary.arbitrary[CantonTimestamp])
      content <- signedProtocolMessageContentArb.arbitrary
      byteStringWithCryptographicEvidence = BytestringWithCryptographicEvidence(
        content.getCryptographicEvidence
      )
    } yield {
      // When we deserialize SignedContent we don't get the original Content, but instead we get a byteStringWithCryptographicEvidence
      // This is why I pass directly a byteStringWithCryptographicEvidence instead of any Content With HasCryptographicEvidence.
      SignedContent.create(
        content = byteStringWithCryptographicEvidence,
        signatures = signatures,
        timestampOfSigningKey = ts,
        representativeProtocolVersion =
          SignedContent.protocolVersionRepresentativeFor(protocolVersion),
      )
    }
  )
}

object GeneratorsProtocol {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*

  implicit val groupRecipientArb: Arbitrary[GroupRecipient] = genArbitrary
  implicit val recipientArb: Arbitrary[Recipient] = genArbitrary
  implicit val memberRecipientArb: Arbitrary[MemberRecipient] = genArbitrary

  implicit val recipientsArb: Arbitrary[Recipients] = {
    val protocolVersionDependentRecipientGen = Arbitrary.arbitrary[Recipient]

    Arbitrary(for {
      depths <- nonEmptyListGen(Arbitrary(Gen.choose(0, 3)))
      trees <- Gen.sequence[List[RecipientsTree], RecipientsTree](
        depths.forgetNE.map(recipientsTreeGen(Arbitrary(protocolVersionDependentRecipientGen)))
      )
    } yield Recipients(NonEmptyUtil.fromUnsafe(trees)))
  }
  implicit val mediatorGroupRecipientArb: Arbitrary[MediatorGroupRecipient] = Arbitrary(
    Arbitrary.arbitrary[NonNegativeInt].map(MediatorGroupRecipient(_))
  )
  implicit val messageIdArb: Arbitrary[MessageId] = Arbitrary(
    Generators.lengthLimitedStringGen(String73).map(s => MessageId.tryCreate(s.str))
  )
  private def recipientsTreeGen(
      recipientArb: Arbitrary[Recipient]
  )(depth: Int): Gen[RecipientsTree] = {
    val maxBreadth = 5
    val recipientGroupGen = nonEmptySetGen(recipientArb)

    if (depth == 0) {
      recipientGroupGen.map(RecipientsTree(_, Nil))
    } else {
      for {
        children <- Gen.listOfN(maxBreadth, recipientsTreeGen(recipientArb)(depth - 1))
        recipientGroup <- recipientGroupGen
      } yield RecipientsTree(recipientGroup, children)
    }
  }
  def deliverGen[Env <: Envelope[?]](
      domainId: DomainId,
      batch: Batch[Env],
      protocolVersion: ProtocolVersion,
  ): Gen[Deliver[Env]] = for {
    timestamp <- Arbitrary.arbitrary[CantonTimestamp]
    counter <- Arbitrary.arbitrary[SequencerCounter]
    messageIdO <- Gen.option(Arbitrary.arbitrary[MessageId])
    topologyTimestampO <- Gen.option(Arbitrary.arbitrary[CantonTimestamp])
    trafficReceipt <- Gen.option(Arbitrary.arbitrary[TrafficReceipt])
  } yield Deliver.create(
    counter,
    timestamp,
    domainId,
    messageIdO,
    batch,
    topologyTimestampO,
    protocolVersion,
    trafficReceipt,
  )

  def timeProofArb(protocolVersion: ProtocolVersion): Arbitrary[TimeProof] = Arbitrary(
    for {
      timestamp <- Arbitrary.arbitrary[CantonTimestamp]
      counter <- nonNegativeLongArb.arbitrary.map(_.unwrap)
      targetDomain <- Arbitrary.arbitrary[Target[DomainId]]
    } yield TimeProofTestUtil.mkTimeProof(timestamp, counter, targetDomain, protocolVersion)
  )
}
