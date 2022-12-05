// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Histogram, Meter}

trait WebSocketMetrics {
  val messagesReceivedTotal: Meter
  val messagesReceivedBytes: Histogram
  val messagesSentTotal: Meter
  val messagesSentBytes: Histogram
}
