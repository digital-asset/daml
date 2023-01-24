// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.error.definitions.LedgerApiErrors.ThreadpoolOverloaded
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}

object ThreadpoolCheck {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  def apply(
      name: String,
      queue: QueueAwareExecutor with NamedExecutor,
      limit: Int,
  ): LimitResultCheck =
    (fullMethodName, _) => {
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
