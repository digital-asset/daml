// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, TargetDomainId, v0}
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.DefaultTestIdentities.domainId
import com.digitalasset.canton.version.{ProtocolVersion, UntypedVersionedMessage}
import com.digitalasset.canton.{BaseTestWordSpec, SequencerCounter}
import com.google.protobuf.ByteString

class SequencedEventTest extends BaseTestWordSpec {
  "serialization" should {

    "correctly serialize and deserialize a deliver event" in {
      // there's no significance to this choice of message beyond it being easy to construct
      val message =
        SignedProtocolMessage.tryFrom(
          TransferResult.create(
            RequestId(CantonTimestamp.now()),
            Set.empty,
            TargetDomainId(domainId),
            Verdict.Approve(testedProtocolVersion),
            testedProtocolVersion,
          ),
          testedProtocolVersion,
          SymbolicCrypto.emptySignature,
        )
      val batch = Batch.of(
        testedProtocolVersion,
        (message, Recipients.cc(DefaultTestIdentities.participant1)),
      )
      val deliver: Deliver[DefaultOpenEnvelope] =
        Deliver.create[DefaultOpenEnvelope](
          SequencerCounter(42),
          CantonTimestamp.now(),
          domainId,
          Some(MessageId.tryCreate("some-message-id")),
          batch,
          testedProtocolVersion,
        )

      if (testedProtocolVersion <= ProtocolVersion.v4) {
        val deliverEventPV0 = deliver.toProtoV0
        val deliverEventP = deliver.toProtoVersioned
        val deserializedEventV0 = deserializeV0(deliverEventPV0)
        val deserializedEvent = deserializeVersioned(deliverEventP)

        deserializedEventV0.value shouldBe deliver
        deserializedEvent.value shouldBe deliver
      } else {
        val deliverEventBS = deliver.toByteString
        val deserializedEvent = deserializeBytestring(deliverEventBS)

        deserializedEvent.value shouldBe deliver
      }
    }

    "correctly serialize and deserialize a deliver error" in {
      val deliverError: DeliverError = DeliverError.create(
        SequencerCounter(42),
        CantonTimestamp.now(),
        domainId,
        MessageId.tryCreate("some-message-id"),
        SequencerErrors.SubmissionRequestRefused("no batches here please"),
        testedProtocolVersion,
      )
      val deliverErrorP = deliverError.toProtoVersioned
      val deserializedEvent = deserializeVersioned(deliverErrorP)

      deserializedEvent.value shouldBe deliverError
    }

    def deserializeV0(
        eventP: v0.SequencedEvent
    ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] = {
      val cryptoPureApi = mock[CryptoPureApi]
      val bytes = eventP.toByteString
      SequencedEvent
        .fromProtoV0(eventP)(bytes)
        .flatMap(closed =>
          new EnvelopeOpener[SequencedEvent](testedProtocolVersion, cryptoPureApi).open(closed)
        )
    }

    def deserializeVersioned(
        eventP: UntypedVersionedMessage
    ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] =
      deserializeBytestring(eventP.toByteString)

    def deserializeBytestring(
        event: ByteString
    ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] = {
      val cryptoPureApi = mock[CryptoPureApi]
      SequencedEvent.fromByteStringOpen(cryptoPureApi, testedProtocolVersion)(event)
    }
  }
}
