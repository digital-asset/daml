// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

final case class SubscriptionResponse(
    signedSequencedEvent: SignedContent[SequencedEvent[ClosedEnvelope]],
    traceContext: TraceContext,
)

object SubscriptionResponse {
  def fromVersionedProtoV30(
      protocolVersion: ProtocolVersion
  )(responseP: v30.VersionedSubscriptionResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[SubscriptionResponse] = {
    val v30.VersionedSubscriptionResponse(
      signedSequencedEvent,
      _ignoredTraceContext,
    ) = responseP
    for {
      signedContent <- SignedContent.fromByteString(protocolVersion, signedSequencedEvent)
      signedSequencedEvent <- signedContent.deserializeContent(
        SequencedEvent.fromByteString(protocolVersion, _)
      )
    } yield SubscriptionResponse(signedSequencedEvent, traceContext)

  }
}
