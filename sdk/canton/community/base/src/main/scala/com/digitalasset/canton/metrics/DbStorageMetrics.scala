// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.metrics.HistogramInventory.Item

class DbStorageHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "db-storage"
  private[metrics] val general = new DbQueueHistograms(prefix :+ "general")
  private[metrics] val write = new DbQueueHistograms(prefix :+ "write")
  private[metrics] val lock = new DbQueueHistograms(prefix :+ "lock")

}

class DbStorageMetrics(
    histograms: DbStorageHistograms,
    metricsFactory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  object general extends DbQueueMetrics(histograms.general, metricsFactory)

  object write extends DbQueueMetrics(histograms.write, metricsFactory)

  object locks extends DbQueueMetrics(histograms.lock, metricsFactory)
}

class DbQueueHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "executor"

  private[metrics] val waitTimer: Item = Item(
    prefix :+ "waittime",
    summary = "Scheduling time metric for database tasks",
    description = """Every database query is scheduled using an asynchronous executor with a queue.
          |The time a task is waiting in this queue is monitored using this metric.""",
    qualification = MetricQualification.Debug,
  )

}

class DbQueueMetrics(
    histograms: DbQueueHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  val prefix = histograms.prefix

  val queue: Counter = factory.counter(
    MetricInfo(
      prefix :+ "queued",
      summary = "Number of database access tasks waiting in queue",
      description =
        """Database access tasks get scheduled in this queue and get executed using one of the
        |existing asynchronous sessions. A large queue indicates that the database connection is
        |not able to deal with the large number of requests.
        |Note that the queue has a maximum size. Tasks that do not fit into the queue
        |will be retried, but won't show up in this metric.""",
      qualification = MetricQualification.Saturation,
    )
  )

  val running: Counter = factory.counter(
    MetricInfo(
      prefix :+ "running",
      summary = "Number of database access tasks currently running",
      description = """Database access tasks run on an async executor. This metric shows
                    |the current number of tasks running in parallel.""",
      qualification = MetricQualification.Debug,
    )
  )

  val waitTimer: Timer = factory.timer(histograms.waitTimer.info)

}
