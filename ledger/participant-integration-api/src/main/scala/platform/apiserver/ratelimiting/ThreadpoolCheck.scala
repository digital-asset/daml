// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.ratelimiting

import com.codahale.metrics.MetricRegistry
import com.daml.error.definitions.LedgerApiErrors.ThreadpoolOverloaded
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricName
import com.daml.platform.apiserver.ratelimiting.LimitResult.{
  LimitResultCheck,
  OverLimit,
  UnderLimit,
}

object ThreadpoolCheck {

  private implicit val logger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(getClass)

  /** Match naming in [[com.codahale.metrics.InstrumentedExecutorService]] */
  final class ThreadpoolCount(metrics: Metrics)(val name: String, val prefix: MetricName) {
    private val submitted = metrics.registry.meter(MetricRegistry.name(prefix, "submitted"))
    private val running = metrics.registry.counter(MetricRegistry.name(prefix, "running"))
    private val completed = metrics.registry.meter(MetricRegistry.name(prefix, "completed"))

    def queueSize: Long = submitted.getCount - running.getCount - completed.getCount
  }

  def apply(count: ThreadpoolCount, limit: Int): LimitResultCheck = {
    apply(count.name, count.prefix, () => count.queueSize, limit)
  }

  def apply(name: String, prefix: String, queueSize: () => Long, limit: Int): LimitResultCheck =
    (fullMethodName, _) => {
      val queued = queueSize()
      if (queued > limit) {
        OverLimit(
          ThreadpoolOverloaded.Rejection(
            name = name,
            queued = queued,
            limit = limit,
            metricPrefix = prefix,
            fullMethodName = fullMethodName,
          )
        )
      } else UnderLimit
    }

}
