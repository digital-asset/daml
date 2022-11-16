// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.http

import com.daml.metrics.api.MetricHandle.{Meter, Timer}

trait HttpMetrics {
  val requestsTotal: Meter
  val errorsTotal: Meter
  val latency: Timer
  val requestsPayloadBytesTotal: Meter
  val responsesPayloadBytesTotal: Meter
}
