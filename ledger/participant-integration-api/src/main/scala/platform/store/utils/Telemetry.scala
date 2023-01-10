// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.utils

import com.daml.ledger.offset.Offset
import com.daml.telemetry.{OpenTelemetryTracer, SpanAttribute}
import io.opentelemetry.api.trace.Span

object Telemetry {

  object Transactions {
    def createSpan(startExclusive: Offset, endInclusive: Offset)(
        fullyQualifiedFunctionName: String
    ): Span =
      OpenTelemetryTracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.OffsetFrom.key, startExclusive.toHexString)
        .setAttribute(SpanAttribute.OffsetTo.key, endInclusive.toHexString)
        .startSpan()

    def createSpan(activeAt: Offset)(fullyQualifiedFunctionName: String): Span =
      OpenTelemetryTracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.Offset.key, activeAt.toHexString)
        .startSpan()

  }
}
