// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.utils

import com.daml.tracing.SpanAttribute
import com.digitalasset.canton.ledger.offset.Offset
import io.opentelemetry.api.trace.{Span, Tracer}

object Telemetry {

  object Transactions {
    def createSpan(tracer: Tracer, startExclusive: Offset, endInclusive: Offset)(
        fullyQualifiedFunctionName: String
    ): Span =
      tracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.OffsetFrom.key, startExclusive.toHexString)
        .setAttribute(SpanAttribute.OffsetTo.key, endInclusive.toHexString)
        .startSpan()

    def createSpan(tracer: Tracer, activeAt: Offset)(fullyQualifiedFunctionName: String): Span =
      tracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.Offset.key, activeAt.toHexString)
        .startSpan()

  }

}
