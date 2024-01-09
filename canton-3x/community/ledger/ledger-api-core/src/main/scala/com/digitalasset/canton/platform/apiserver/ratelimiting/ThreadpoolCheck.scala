// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.digitalasset.canton.ledger.error.LedgerApiErrors.ThreadpoolOverloaded
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}
import com.digitalasset.canton.tracing.TraceContext

object ThreadpoolCheck {

  def apply(
      name: String,
      queue: QueueAwareExecutor with NamedExecutor,
      limit: Int,
      loggerFactory: NamedLoggerFactory,
  ): LimitResultCheck =
    (fullMethodName, _) => {
      implicit val errorLogger = ErrorLoggingContext(
        loggerFactory.getTracedLogger(getClass),
        loggerFactory.properties,
        TraceContext.empty,
      )
      val queued = queue.queueSize
      if (queued > limit) {
        OverLimit(
          ThreadpoolOverloaded.Rejection(
            name = name,
            queued = queued,
            limit = limit,
            fullMethodName = fullMethodName,
            metricNameLabel = queue.name,
          )
        )
      } else UnderLimit
    }

}
