// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.trace.TracingContextUtils

/**
 * A wafer-thin abstraction over OpenTelemetry so other packages don't need to
 * use `opentelemetry-api` directly.
 */
object Spans {
  def addEventToCurrentSpan(event: Event): Unit =
    TracingContextUtils.getCurrentSpan.addEvent(event)

  def setCurrentSpanAttribute(key: String, value: String): Unit =
    TracingContextUtils.getCurrentSpan.setAttribute(key, value)
}
