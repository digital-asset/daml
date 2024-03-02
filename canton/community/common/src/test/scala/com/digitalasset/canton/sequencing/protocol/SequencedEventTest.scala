// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{CryptoPureApi, TestHash}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.DefaultTestIdentities.domainId
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.digitalasset.canton.{BaseTestWordSpec, SequencerCounter}
import com.google.protobuf.ByteString

class SequencedEventTest extends BaseTestWordSpec {
  "serialization" should {

    "correctly serialize and deserialize a deliver event" in {
      // there's no significance to this choice of message beyond it being easy to construct
      val message =
        SignedProtocolMessage.from(
          ConfirmationResultMessage.create(
            domainId,
            ViewType.TransferOutViewType,
            RequestId(CantonTimestamp.now()),
            Some(TestHash.dummyRootHash),
            Verdict.Approve(testedProtocolVersion),
            Set.empty,
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
          Some(CantonTimestamp.ofEpochSecond(1)),
          testedProtocolVersion,
        )

      val deliverEventBS = deliver.toByteString
      val deserializedEvent = deserializeBytestring(deliverEventBS)

      deserializedEvent.value shouldBe deliver
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
