// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.sequencing.sequencer.store.{
  DeliverStoreEvent,
  Payload,
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
      payload: Payload = Payload(
        PayloadId(CantonTimestamp.Epoch),
        Batch.empty(BaseTest.testedProtocolVersion).toByteString,
      ),
      signingTs: Option[CantonTimestamp] = None,
      traceContext: TraceContext = TraceContext.empty,
      trafficReceiptO: Option[TrafficReceipt] = None,
  )(
      recipients: NonEmpty[SortedSet[SequencerMemberId]] = NonEmpty(SortedSet, sender)
  ): DeliverStoreEvent[Payload] = {
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

  def payloadsForEvents(events: Seq[Sequenced[PayloadId]]): List[Payload] = {
    val payloadIds = events.mapFilter(_.event.payloadO)
    payloadIds
      .map(pid => Payload(pid, Batch.empty(BaseTest.testedProtocolVersion).toByteString))
      .toList
  }
}
