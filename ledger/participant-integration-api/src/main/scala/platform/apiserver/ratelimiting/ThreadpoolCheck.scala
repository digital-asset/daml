// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.daml.error.definitions.LedgerApiErrors.ThreadpoolOverloaded
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}

object ThreadpoolCheck {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  final case class ThreadpoolCount(
      name: String,
      metricNameLabel: String,
      queueSizeProvider: () => Long,
  )

  def apply(count: ThreadpoolCount, limit: Int): LimitResultCheck =
    (fullMethodName, _) => {
      val queued = count.queueSizeProvider()
      if (queued > limit) {
        OverLimit(
          ThreadpoolOverloaded.Rejection(
            name = count.name,
            queued = queued,
            limit = limit,
            fullMethodName = fullMethodName,
            metricNameLabel = count.metricNameLabel,
          )
        )
      } else UnderLimit
    }

}
