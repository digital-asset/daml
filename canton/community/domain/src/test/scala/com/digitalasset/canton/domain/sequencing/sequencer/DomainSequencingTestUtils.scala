// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store.*
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.immutable.SortedSet

object DomainSequencingTestUtils {

  def mockDeliverStoreEvent(
      sender: SequencerMemberId = SequencerMemberId(0),
      payloadId: PayloadId = PayloadId(CantonTimestamp.Epoch),
      signingTs: Option[CantonTimestamp] = None,
      traceContext: TraceContext = TraceContext.empty,
  )(
      recipients: NonEmpty[SortedSet[SequencerMemberId]] = NonEmpty(SortedSet, sender)
  ): DeliverStoreEvent[PayloadId] = {
    val messageId = MessageId.tryCreate("mock-deliver")
    DeliverStoreEvent(sender, messageId, recipients, payloadId, signingTs, traceContext)
  }

  def payloadsForEvents(events: Seq[Sequenced[PayloadId]]): List[Payload] = {
    val payloadIds = events.mapFilter(_.event.payloadO)
    payloadIds
      .map(pid => Payload(pid, Batch.empty(BaseTest.testedProtocolVersion).toByteString))
      .toList
  }
}
