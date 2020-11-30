// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.opentelemetry.trace.TracingContextUtils

object Spans {
  def addEventToCurrentSpan(event: Event): Unit =
    TracingContextUtils.getCurrentSpan.addEvent(event)

  def setCurrentSpanAttribute(key: String, value: String): Unit =
    TracingContextUtils.getCurrentSpan.setAttribute(key, value)
}
