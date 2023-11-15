// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.{GeneratorsMessages, ProtocolMessage}
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.{GeneratorsVersion, ProtocolVersion}
import com.digitalasset.canton.{Generators, SequencerCounter}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsProtocol {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*

  implicit val acknowledgeRequestArb: Arbitrary[AcknowledgeRequest] = Arbitrary(for {
    protocolVersion <- representativeProtocolVersionGen(AcknowledgeRequest)
    ts <- Arbitrary.arbitrary[CantonTimestamp]
    member <- Arbitrary.arbitrary[Member]
  } yield AcknowledgeRequest(member, ts, protocolVersion.representative))

  implicit val aggregationRuleArb: Arbitrary[AggregationRule] = Arbitrary(for {
    rpv <- GeneratorsVersion.representativeProtocolVersionGen[AggregationRule](AggregationRule)
    threshold <- Arbitrary.arbitrary[PositiveInt]
    eligibleMembers <- Generators.nonEmptyListGen[Member]
  } yield AggregationRule(eligibleMembers, threshold)(rpv))

  implicit val groupRecipientArb: Arbitrary[GroupRecipient] = genArbitrary
  implicit val recipientArb: Arbitrary[Recipient] = genArbitrary
  implicit val memberRecipientArb: Arbitrary[MemberRecipient] = genArbitrary

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

  def recipientsArb(protocolVersion: ProtocolVersion): Arbitrary[Recipients] = {

    // For pv < ClosedEnvelope.groupAddressesSupportedSince, the recipients should contain only members
    val protocolVersionDependentRecipientGen =
      if (protocolVersion < ClosedEnvelope.groupAddressesSupportedSince.representative) {
        Arbitrary.arbitrary[MemberRecipient]
      } else
        Arbitrary.arbitrary[Recipient]

    Arbitrary(for {
      depths <- nonEmptyListGen(Arbitrary(Gen.choose(0, 3)))
      trees <- Gen.sequence[List[RecipientsTree], RecipientsTree](
        depths.forgetNE.map(recipientsTreeGen(Arbitrary(protocolVersionDependentRecipientGen)))
      )
    } yield Recipients(NonEmptyUtil.fromUnsafe(trees)))
  }

  private def closedEnvelopeGenFor(protocolVersion: ProtocolVersion): Gen[ClosedEnvelope] =
    for {
      bytes <- Arbitrary.arbitrary[ByteString]
      signatures <- defaultValueGen(protocolVersion, ClosedEnvelope.defaultSignaturesUntil)(
        Arbitrary(Gen.listOfN(5, signatureArb.arbitrary))
      )
      recipients <- recipientsArb(protocolVersion).arbitrary
    } yield ClosedEnvelope.tryCreate(bytes, recipients, signatures, protocolVersion)

  implicit val closedEnvelopeArb: Arbitrary[ClosedEnvelope] = Arbitrary(for {
    protocolVersion <- Arbitrary.arbitrary[ProtocolVersion]
    res <- closedEnvelopeGenFor(protocolVersion)
  } yield res)

  implicit val mediatorsOfDomainArb: Arbitrary[MediatorsOfDomain] = Arbitrary(
    Arbitrary.arbitrary[NonNegativeInt].map(MediatorsOfDomain(_))
  )

  implicit val messageIdArb: Arbitrary[MessageId] = Arbitrary(
    Generators.lengthLimitedStringGen(String73).map(s => MessageId.tryCreate(s.str))
  )

  private def openEnvelopGen(protocolVersion: ProtocolVersion): Gen[OpenEnvelope[ProtocolMessage]] =
    for {
      protocolMessage <- GeneratorsMessages.protocolMessageGen(protocolVersion)
      recipients <- recipientsArb(protocolVersion).arbitrary

    } yield OpenEnvelope(protocolMessage, recipients)(protocolVersion)

  private def envelopeGenFor(pv: ProtocolVersion): Arbitrary[Envelope[?]] =
    Arbitrary(Gen.oneOf[Envelope[?]](closedEnvelopeGenFor(pv), openEnvelopGen(pv)))

  def deliverGen[Env <: Envelope[?]](
      domainId: DomainId,
      batch: Batch[Env],
      protocolVersion: ProtocolVersion,
  ): Gen[Deliver[Env]] = for {
    timestamp <- Arbitrary.arbitrary[CantonTimestamp]
    counter <- Arbitrary.arbitrary[SequencerCounter]
    messageIdO <- Gen.option(Arbitrary.arbitrary[MessageId])
  } yield Deliver.create(
    counter,
    timestamp,
    domainId,
    messageIdO,
    batch,
    protocolVersion,
  )

  implicit val batchArb: Arbitrary[Batch[Envelope[?]]] =
    Arbitrary(for {
      protocolVersion <- Arbitrary.arbitrary[ProtocolVersion]
      envelopes <- Generators.nonEmptyListGen[Envelope[?]](
        envelopeGenFor(protocolVersion)
      )
    } yield Batch(envelopes.map(_.closeEnvelope), protocolVersion))

}
