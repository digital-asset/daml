// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

class DbStorageHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "db-storage"
  private[metrics] val general = new DbQueueHistograms(prefix :+ "general")
  private[metrics] val write = new DbQueueHistograms(prefix :+ "write")

}

class DbStorageMetrics(
    histograms: DbStorageHistograms,
    metricsFactory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  val general: DbQueueMetrics = new DbQueueMetrics(histograms.general, metricsFactory)

  val write: DbQueueMetrics = new DbQueueMetrics(histograms.write, metricsFactory)

}

private[metrics] class DbQueueHistograms(val parent: MetricName)(implicit
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

  private[metrics] val execTimer: Item = Item(
    prefix :+ "exectime",
    summary = "Execution time metric for database tasks",
    description = """The time a task is running on the database is measured using this metric.""",
    qualification = MetricQualification.Debug,
  )

}

class DbQueueMetrics(
    histograms: DbQueueHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  val prefix: MetricName = histograms.prefix

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

  val running: Gauge[Int] = factory.gauge(
    MetricInfo(
      prefix :+ "running",
      summary = "Number of database access tasks currently running",
      description = """Database access tasks run on an async executor. This metric shows
                    |the current number of tasks running in parallel.""",
      qualification = MetricQualification.Debug,
    ),
    0,
  )

  val waitTimer: Timer = factory.timer(histograms.waitTimer.info)

  val load: Gauge[Double] = factory.gauge(
    MetricInfo(
      prefix :+ "load",
      "Load of database pool",
      MetricQualification.Saturation,
      """Database queries run as tasks on an async executor. This metric shows
the current number of queries running in parallel divided by the number
database connections for this database connection pool.""",
    ),
    0.0,
  )

  val execTimer: Timer = factory.timer(histograms.execTimer.info)

}
