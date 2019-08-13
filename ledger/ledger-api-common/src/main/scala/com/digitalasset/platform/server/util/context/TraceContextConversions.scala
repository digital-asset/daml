// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.util.context

import brave.propagation.{TraceContext => BraveContext}
import com.digitalasset.ledger.api.v1.trace_context.{TraceContext => ProtoContext}

object TraceContextConversions {

  def toProto(braveContext: BraveContext) =
    ProtoContext(
      traceIdHigh = braveContext.traceIdHigh(),
      traceId = braveContext.traceId(),
      spanId = braveContext.spanId(),
      parentSpanId =
        if (braveContext.parentId == null) None else Some(braveContext.parentId.toLong),
      sampled = braveContext.sampled()
    )

  def toBrave(protoContext: ProtoContext): BraveContext =
    BraveContext
      .newBuilder()
      .traceIdHigh(protoContext.traceIdHigh)
      .traceId(protoContext.traceId)
      .spanId(protoContext.spanId)
      .parentId(protoContext.parentSpanId.fold[java.lang.Long](null)(java.lang.Long.valueOf))
      .sampled(protoContext.sampled)
      .shared(true) // Assume the client started the span that's being converted, since it's coming from a grpc payload.
      .build()
}
