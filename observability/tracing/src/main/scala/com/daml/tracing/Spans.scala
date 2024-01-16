// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tracing

import io.opentelemetry.api.trace.Span

/** A wafer-thin abstraction over OpenTelemetry so other packages don't need to
  * use `opentelemetry-api` directly.
  */
object Spans {
  def addEventToCurrentSpan(event: Event): Unit = {
    addEventToSpan(event, Span.current)
  }

  def addEventToSpan(event: Event, span: Span): Unit = {
    val _ = span.addEvent(event.name, event.getAttributes)
  }

  def setCurrentSpanAttribute(attribute: SpanAttribute, value: String): Unit = {
    val _ = Span.current.setAttribute(attribute.key, value)
  }
}
