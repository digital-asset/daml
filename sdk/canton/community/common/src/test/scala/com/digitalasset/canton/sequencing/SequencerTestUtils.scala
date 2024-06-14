// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  Deliver,
  DeliverError,
  MessageId,
  SequencedEvent,
  SequencerDeliverError,
  SignedContent,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.{DefaultTestIdentities, DomainId}
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import com.google.protobuf.ByteString

object SequencerTestUtils extends BaseTest {

  object MockMessageContent {
    private val bytes = ByteString.copyFromUtf8("serialized-mock-message")
    def toByteString: ByteString = bytes
  }

  def sign[M <: ProtocolVersionedMemoizedEvidence](content: M): SignedContent[M] =
    SignedContent(content, SymbolicCrypto.emptySignature, None, testedProtocolVersion)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def mockDeliverClosedEnvelope(
      counter: Long = 0L,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
      domainId: DomainId = DefaultTestIdentities.domainId,
      deserializedFrom: Option[ByteString] = None,
      messageId: Option[MessageId] = Some(MessageId.tryCreate("mock-deliver")),
      topologyTimestampO: Option[CantonTimestamp] = None,
  ): Deliver[ClosedEnvelope] = {
    val batch = Batch.empty(testedProtocolVersion)

    val deliver = Deliver.create[ClosedEnvelope](
      SequencerCounter(counter),
      timestamp,
      domainId,
      messageId,
      batch,
      topologyTimestampO,
      testedProtocolVersion,
      Option.empty[TrafficReceipt],
    )

    deserializedFrom match {
      case Some(bytes) =>
        // Somehow ugly way to tweak the `deserializedFrom` attribute of Deliver
        SequencedEvent
          .fromProtoV30(deliver.toProtoV30)(bytes)
          .value
          .asInstanceOf[Deliver[ClosedEnvelope]]

      case None => deliver
    }
  }

  def mockDeliver(
      sc: Long = 0,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
      domainId: DomainId = DefaultTestIdentities.domainId,
      messageId: Option[MessageId] = Some(MessageId.tryCreate("mock-deliver")),
      topologyTimestampO: Option[CantonTimestamp] = None,
      trafficReceipt: Option[TrafficReceipt] = None,
  ): Deliver[Nothing] = {
    val batch = Batch.empty(testedProtocolVersion)
    Deliver.create[Nothing](
      SequencerCounter(sc),
      timestamp,
      domainId,
      messageId,
      batch,
      topologyTimestampO,
      BaseTest.testedProtocolVersion,
      trafficReceipt,
    )
  }

  def mockDeliverError(
      sc: Long = 0,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
      domainId: DomainId = DefaultTestIdentities.domainId,
      messageId: MessageId = MessageId.tryCreate("mock-deliver"),
      sequencerError: SequencerDeliverError = SubmissionRequestRefused("mock-submission-refused"),
      trafficReceipt: Option[TrafficReceipt] = None,
  ): DeliverError = {
    DeliverError.create(
      SequencerCounter(sc),
      timestamp,
      domainId,
      messageId,
      sequencerError,
      BaseTest.testedProtocolVersion,
      trafficReceipt,
    )
  }

}
