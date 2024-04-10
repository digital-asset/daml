// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

final case class SubscriptionResponse(
    signedSequencedEvent: SignedContent[SequencedEvent[ClosedEnvelope]],
    traceContext: TraceContext,
    trafficState: Option[SequencedEventTrafficState],
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
      trafficStateP,
    ) = responseP
    for {
      signedContent <- SignedContent.fromByteString(protocolVersion)(
        signedSequencedEvent
      )
      signedSequencedEvent <- signedContent.deserializeContent(
        SequencedEvent.fromByteString(protocolVersion)
      )
      trafficState <- trafficStateP.traverse(SequencedEventTrafficState.fromProtoV30)
    } yield SubscriptionResponse(signedSequencedEvent, traceContext, trafficState)

  }
}
