// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.api.MetricHandle.{Histogram, Meter, Timer}

trait HttpMetrics {
  val requestsTotal: Meter
  val latency: Timer
  val requestsPayloadBytes: Histogram
  val responsesPayloadBytes: Histogram
}
