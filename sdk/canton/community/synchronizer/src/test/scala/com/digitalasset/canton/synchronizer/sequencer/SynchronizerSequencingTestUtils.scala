// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.sequencer.store.{
  BytesPayload,
  DeliverStoreEvent,
  PayloadId,
  Sequenced,
  SequencerMemberId,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.immutable.SortedSet

object SynchronizerSequencingTestUtils {

  def mockDeliverStoreEvent(
      sender: SequencerMemberId = SequencerMemberId(0),
      payloadId: PayloadId = PayloadId(CantonTimestamp.Epoch),
      signingTs: Option[CantonTimestamp] = None,
      traceContext: TraceContext = TraceContext.empty,
      trafficReceiptO: Option[TrafficReceipt] = None,
  )(
      recipients: NonEmpty[SortedSet[SequencerMemberId]] = NonEmpty(SortedSet, sender)
  ): DeliverStoreEvent[PayloadId] = {
    val messageId = MessageId.tryCreate("mock-deliver")
    DeliverStoreEvent(
      sender,
      messageId,
      recipients,
      payloadId,
      signingTs,
      traceContext,
      trafficReceiptO,
    )
  }

  def deliverStoreEventWithPayloadWithDefaults(
      sender: SequencerMemberId = SequencerMemberId(0),
      payload: BytesPayload = BytesPayload(
        PayloadId(CantonTimestamp.Epoch),
        Batch.empty(BaseTest.testedProtocolVersion).toByteString,
      ),
      signingTs: Option[CantonTimestamp] = None,
      traceContext: TraceContext = TraceContext.empty,
      trafficReceiptO: Option[TrafficReceipt] = None,
  )(
      recipients: NonEmpty[SortedSet[SequencerMemberId]] = NonEmpty(SortedSet, sender)
  ): DeliverStoreEvent[BytesPayload] = {
    val messageId = MessageId.tryCreate("mock-deliver")
    DeliverStoreEvent(
      sender,
      messageId,
      recipients,
      payload,
      signingTs,
      traceContext,
      trafficReceiptO,
    )
  }

  def payloadsForEvents(events: Seq[Sequenced[PayloadId]]): List[BytesPayload] = {
    val payloadIds = events.mapFilter(_.event.payloadO)
    payloadIds
      .map(pid => BytesPayload(pid, Batch.empty(BaseTest.testedProtocolVersion).toByteString))
      .toList
  }
}
