// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.utils

import com.daml.tracing.SpanAttribute
import com.digitalasset.canton.data.Offset
import io.opentelemetry.api.trace.{Span, Tracer}

object Telemetry {

  object Transactions {
    def createSpan(tracer: Tracer, startInclusive: Offset, endInclusive: Offset)(
        fullyQualifiedFunctionName: String
    ): Span =
      tracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.OffsetFrom.key, startInclusive.toDecimalString)
        .setAttribute(SpanAttribute.OffsetTo.key, endInclusive.toDecimalString)
        .startSpan()

    def createSpan(tracer: Tracer, activeAt: Offset)(
        fullyQualifiedFunctionName: String
    ): Span =
      tracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.Offset.key, activeAt.toDecimalString)
        .startSpan()

  }

}
