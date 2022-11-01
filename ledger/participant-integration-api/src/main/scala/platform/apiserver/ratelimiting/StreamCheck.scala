// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.metrics.api.MetricHandle.Counter
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
      activeStreamsCounter: Counter,
      activeStreamsName: MetricName,
      maxStreams: Int,
  ): LimitResultCheck = (fullMethodName, isStream) => {
    if (isStream) {
      if (activeStreamsCounter.getCount >= maxStreams) {
        OverLimit(
          MaximumNumberOfStreams.Rejection(
            value = activeStreamsCounter.getCount,
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
