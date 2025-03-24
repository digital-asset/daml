// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{AsymmetricEncrypted, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ProtocolSymmetricKey
import com.digitalasset.canton.protocol.messages.{GeneratorsMessages, ProtocolMessage}
import com.digitalasset.canton.sequencing.channel.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
}
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelConnectedToAllEndpoints,
  SequencerChannelId,
  SequencerChannelMetadata,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyAck,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
}
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.{GeneratorsVersion, ProtocolVersion}
import com.digitalasset.canton.{Generators, SequencerCounter}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsProtocol(
    protocolVersion: ProtocolVersion,
    generatorsMessages: GeneratorsMessages,
) {
  import GeneratorsProtocol.*
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsMessages.*

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

  implicit val subscriptionRequestV2Arb: Arbitrary[SubscriptionRequestV2] = Arbitrary(
    for {
      member <- Arbitrary.arbitrary[Member]
      timestamp <- Arbitrary.arbitrary[Option[CantonTimestamp]]
    } yield SubscriptionRequestV2.apply(member, timestamp, protocolVersion)
  )

  implicit val sequencerChannelMetadataArb: Arbitrary[SequencerChannelMetadata] =
    Arbitrary(
      for {
        channelId <- Arbitrary.arbitrary[SequencerChannelId]
        initiatingMember <- Arbitrary.arbitrary[Member]
        receivingMember <- Arbitrary.arbitrary[Member]
      } yield SequencerChannelMetadata.apply(
        channelId,
        initiatingMember,
        receivingMember,
        protocolVersion,
      )
    )

  implicit val sequencerChannelConnectedToAllEndpointsArb
      : Arbitrary[SequencerChannelConnectedToAllEndpoints] =
    Arbitrary(
      SequencerChannelConnectedToAllEndpoints.apply(
        protocolVersion
      )
    )

  implicit val sequencerChannelSessionKeyArb: Arbitrary[SequencerChannelSessionKey] =
    Arbitrary(for {
      encrypted <- Arbitrary.arbitrary[AsymmetricEncrypted[ProtocolSymmetricKey]]
    } yield SequencerChannelSessionKey.apply(encrypted, protocolVersion))

  implicit val sequencerChannelSessionKeyAckArb: Arbitrary[SequencerChannelSessionKeyAck] =
    Arbitrary(SequencerChannelSessionKeyAck.apply(protocolVersion))

  implicit val connectToSequencerChannelRequestArb: Arbitrary[ConnectToSequencerChannelRequest] =
    Arbitrary(
      for {
        request <- Gen.oneOf(
          byteStringArb.arbitrary.map(
            ConnectToSequencerChannelRequest.Payload(_): ConnectToSequencerChannelRequest.Request
          ),
          sequencerChannelMetadataArb.arbitrary.map(ConnectToSequencerChannelRequest.Metadata.apply),
        )
      } yield ConnectToSequencerChannelRequest.apply(
        request,
        TraceContext.empty,
        protocolVersion,
      )
    )

  implicit val connectToSequencerChannelResponseArb: Arbitrary[ConnectToSequencerChannelResponse] =
    Arbitrary(
      for {
        response <- Gen.oneOf(
          byteStringArb.arbitrary.map(
            ConnectToSequencerChannelResponse.Payload(_): ConnectToSequencerChannelResponse.Response
          ),
          sequencerChannelConnectedToAllEndpointsArb.arbitrary.map(_ =>
            ConnectToSequencerChannelResponse.Connected
          ),
        )
      } yield ConnectToSequencerChannelResponse.apply(
        response,
        TraceContext.empty,
        protocolVersion,
      )
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
      pts <- Arbitrary.arbitrary[Option[CantonTimestamp]]
      ts <- Arbitrary.arbitrary[CantonTimestamp]
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
      messageId <- Arbitrary.arbitrary[MessageId]
      error <- sequencerDeliverErrorArb.arbitrary
    } yield DeliverError.create(
      sequencerCounter,
      previousTimestamp = pts,
      timestamp = ts,
      synchronizerId = synchronizerId,
      messageId,
      error,
      protocolVersion,
      Option.empty[TrafficReceipt],
    )
  )
  private implicit val deliverArbitrary: Arbitrary[Deliver[Envelope[?]]] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
      batch <- batchArb.arbitrary
      deliver <- Arbitrary(deliverGen(synchronizerId, batch, protocolVersion)).arbitrary
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
      synchronizerId: SynchronizerId,
      batch: Batch[Env],
      protocolVersion: ProtocolVersion,
  ): Gen[Deliver[Env]] = for {
    previousTimestamp <- Arbitrary.arbitrary[Option[CantonTimestamp]]
    timestamp <- Arbitrary.arbitrary[CantonTimestamp]
    counter <- Arbitrary.arbitrary[SequencerCounter]
    messageIdO <- Gen.option(Arbitrary.arbitrary[MessageId])
    topologyTimestampO <- Gen.option(Arbitrary.arbitrary[CantonTimestamp])
    trafficReceipt <- Gen.option(Arbitrary.arbitrary[TrafficReceipt])
  } yield Deliver.create(
    counter,
    previousTimestamp,
    timestamp,
    synchronizerId,
    messageIdO,
    batch,
    topologyTimestampO,
    protocolVersion,
    trafficReceipt,
  )

  def timeProofArb(protocolVersion: ProtocolVersion): Arbitrary[TimeProof] = Arbitrary(
    for {
      timestamp <- Arbitrary.arbitrary[CantonTimestamp]
      previousEventTimestamp <- Arbitrary.arbitrary[Option[CantonTimestamp]]
      counter <- nonNegativeLongArb.arbitrary.map(_.unwrap)
      targetSynchronizerId <- Arbitrary.arbitrary[Target[SynchronizerId]]
    } yield TimeProofTestUtil.mkTimeProof(
      timestamp,
      previousEventTimestamp,
      counter,
      targetSynchronizerId,
      protocolVersion,
    )
  )
}
