// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.error.definitions.LedgerApiErrors.MaximumNumberOfStreams
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.MetricName
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}

object StreamCheck {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  def apply(
      activeStreamsGauge: Gauge[Int],
      activeStreamsName: MetricName,
      maxStreams: Int,
  ): LimitResultCheck = (fullMethodName, isStream) => {
    if (isStream) {
      if (activeStreamsGauge.getValue >= maxStreams) {
        OverLimit(
          MaximumNumberOfStreams.Rejection(
            value = activeStreamsGauge.getValue.toLong,
            limit = maxStreams,
            metricPrefix = activeStreamsName,
            fullMethodName = fullMethodName,
          )
        )
      } else {
        UnderLimit
      }
    } else {
      UnderLimit
    }
  }

}
