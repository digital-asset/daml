// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, required}
import com.digitalasset.canton.tracing.TraceContext

final case class SubscriptionResponse(
    signedSequencedEvent: SignedContent[SequencedEvent[ClosedEnvelope]],
    traceContext: TraceContext,
    trafficState: Option[SequencedEventTrafficState],
)

object SubscriptionResponse {

  /** Deserializes the SubscriptionResponse however will ignore the
    * [[com.digitalasset.canton.domain.api.v0.SubscriptionResponse.traceContext]] field and instead use the supplied value.
    * This is because we deserialize the traceContext separately from this request immediately on receiving the structure.
    */
  def fromProtoV0(responseP: v0.SubscriptionResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[SubscriptionResponse] = {
    val v0.SubscriptionResponse(
      maybeSignedSequencedEventP,
      _ignoredTraceContext,
    ) = responseP
    for {
      signedSequencedEventP <- required(
        "SubscriptionResponse.signedSequencedEvent",
        maybeSignedSequencedEventP,
      )
      signedContent <- SignedContent.fromProtoV0(signedSequencedEventP)
      signedSequencedEvent <- signedContent.deserializeContent(SequencedEvent.fromByteString)
    } yield SubscriptionResponse(signedSequencedEvent, traceContext, None)
  }

  def fromVersionedProtoV0(responseP: v0.VersionedSubscriptionResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[SubscriptionResponse] = {
    val v0.VersionedSubscriptionResponse(
      signedSequencedEvent,
      _ignoredTraceContext,
      trafficStateP,
    ) = responseP
    for {
      signedContent <- SignedContent.fromByteString(signedSequencedEvent)
      signedSequencedEvent <- signedContent.deserializeContent(SequencedEvent.fromByteString)
      trafficState <- trafficStateP.traverse(SequencedEventTrafficState.fromProtoV0)
    } yield SubscriptionResponse(signedSequencedEvent, traceContext, trafficState)

  }
}
