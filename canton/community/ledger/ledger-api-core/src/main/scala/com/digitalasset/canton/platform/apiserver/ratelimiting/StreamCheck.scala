// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.ledger.error.LedgerApiErrors.MaximumNumberOfStreams
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}
import com.digitalasset.canton.tracing.TraceContext

object StreamCheck {

  def apply(
      activeStreamsGauge: Gauge[Int],
      activeStreamsName: MetricName,
      maxStreams: Int,
      loggerFactory: NamedLoggerFactory,
  ): LimitResultCheck = (fullMethodName, isStream) => {
    implicit val errorLogger = ErrorLoggingContext(
      loggerFactory.getTracedLogger(getClass),
      loggerFactory.properties,
      TraceContext.empty,
    )

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
