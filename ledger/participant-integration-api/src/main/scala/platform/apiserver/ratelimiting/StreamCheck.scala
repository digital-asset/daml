// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.error.definitions.LedgerApiErrors.MaximumNumberOfStreams
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}

object StreamCheck {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  def apply(
      activeStreamNumber: () => Long,
      activeStreamsName: String,
      maxStreams: Int,
  ): LimitResultCheck = (fullMethodName, isStream) => {
    if (isStream) {
      val activeStreams = activeStreamNumber()
      if (activeStreams >= maxStreams) {
        OverLimit(
          MaximumNumberOfStreams.Rejection(
            value = activeStreams,
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
