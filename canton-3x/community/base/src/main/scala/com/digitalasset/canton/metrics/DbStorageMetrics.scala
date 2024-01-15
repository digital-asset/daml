// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Timer}
import com.daml.metrics.api.noop.{NoOpGauge, NoOpTimer}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}
import com.digitalasset.canton.metrics.MetricHandle.LabeledMetricsFactory

import scala.concurrent.duration.*

@MetricDoc.GroupTag(
  representative = "canton.db-storage.<service>.executor",
  groupableClass = classOf[DbQueueMetrics],
)
class DbStorageMetrics(
    basePrefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
) {

  val prefix: MetricName = basePrefix :+ "db-storage"

  def loadGaugeM(name: String): TimedLoadGauge = {
    val timerM = metricsFactory.timer(prefix :+ name)
    metricsFactory.loadGauge(prefix :+ name :+ "load", 1.second, timerM)(MetricsContext.Empty)
  }

  @MetricDoc.Tag(
    summary = "Timer monitoring duration and rate of accessing the given storage",
    description = """Covers both read from and writes to the storage.""",
    qualification = Debug,
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val timerExampleForDocs: Timer = NoOpTimer(prefix :+ "<storage>")

  @MetricDoc.Tag(
    summary = "The load on the given storage",
    description =
      """The load is a factor between 0 and 1 describing how much of an existing interval
          |has been spent reading from or writing to the storage.""",
    qualification = Debug,
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val loadExampleForDocs: Gauge[Double] =
    NoOpGauge(prefix :+ "<storage>" :+ "load", 0d)

  object alerts extends DbAlertMetrics(prefix, metricsFactory)

  object queue extends DbQueueMetrics(prefix :+ "general", metricsFactory)

  object writeQueue extends DbQueueMetrics(prefix :+ "write", metricsFactory)

  object locks extends DbQueueMetrics(prefix :+ "locks", metricsFactory)
}

class DbQueueMetrics(
    basePrefix: MetricName,
    factory: LabeledMetricsFactory,
) {
  val prefix: MetricName = basePrefix :+ "executor"

  @MetricDoc.Tag(
    summary = "Number of database access tasks waiting in queue",
    description =
      """Database access tasks get scheduled in this queue and get executed using one of the
        |existing asynchronous sessions. A large queue indicates that the database connection is
        |not able to deal with the large number of requests.
        |Note that the queue has a maximum size. Tasks that do not fit into the queue
        |will be retried, but won't show up in this metric.""",
    qualification = Debug,
  )
  val queue: Counter = factory.counter(prefix :+ "queued")

  @MetricDoc.Tag(
    summary = "Number of database access tasks currently running",
    description = """Database access tasks run on an async executor. This metric shows
        |the current number of tasks running in parallel.""",
    qualification = Debug,
  )
  val running: Counter = factory.counter(prefix :+ "running")

  @MetricDoc.Tag(
    summary = "Scheduling time metric for database tasks",
    description = """Every database query is scheduled using an asynchronous executor with a queue.
        |The time a task is waiting in this queue is monitored using this metric.""",
    qualification = Debug,
  )
  val waitTimer: Timer = factory.timer(prefix :+ "waittime")

}

class DbAlertMetrics(
    basePrefix: MetricName,
    factory: LabeledMetricsFactory,
) {
  val prefix: MetricName = basePrefix :+ "alerts"

  @MetricDoc.Tag(
    summary = "Number of failed writes to the event log",
    description =
      """Failed writes to the single dimension event log indicate an issue requiring user intervention. In the case of
        |domain event logs, the corresponding domain no longer emits any subsequent events until domain recovery is
        |initiated (e.g. by disconnecting and reconnecting the participant from the domain). In the case of the
        |participant event log, an operation might need to be reissued. If this counter is larger than zero, check the
        |canton log for errors for details.
        |""",
    qualification = Debug,
  )
  val failedEventLogWrites: Counter = factory.counter(prefix :+ "single-dimension-event-log")

  @MetricDoc.Tag(
    summary = "Number of failed writes to the multi-domain event log",
    description =
      """Failed writes to the multi domain event log indicate an issue requiring user intervention. In the case of
        |domain event logs, the corresponding domain no longer emits any subsequent events until domain recovery is
        |initiated (e.g. by disconnecting and reconnecting the participant from the domain). In the case of the
        |participant event log, an operation might need to be reissued. If this counter is larger than zero, check the
        |canton log for errors for details.
        |""",
    qualification = Debug,
  )
  val failedMultiDomainEventLogWrites: Counter = factory.counter(prefix :+ "multi-domain-event-log")
}
