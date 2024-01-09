// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

final case class SubscriptionResponse(
    signedSequencedEvent: SignedContent[SequencedEvent[ClosedEnvelope]],
    traceContext: TraceContext,
    trafficState: Option[SequencedEventTrafficState],
)

object SubscriptionResponse {
  def fromVersionedProtoV0(
      protocolVersion: ProtocolVersion
  )(responseP: v0.VersionedSubscriptionResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[SubscriptionResponse] = {
    val v0.VersionedSubscriptionResponse(
      signedSequencedEvent,
      _ignoredTraceContext,
      trafficStateP,
    ) = responseP
    for {
      signedContent <- SignedContent.fromByteString(protocolVersion)(
        signedSequencedEvent
      )
      signedSequencedEvent <- signedContent.deserializeContent(
        SequencedEvent.fromByteString(protocolVersion)
      )
      trafficState <- trafficStateP.traverse(SequencedEventTrafficState.fromProtoV0)
    } yield SubscriptionResponse(signedSequencedEvent, traceContext, trafficState)

  }
}
