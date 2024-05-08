// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.daml.metrics.api.HistogramInventory
import com.daml.metrics.api.HistogramInventory.Item

class CommitmentHistograms(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "commitments"
  private[metrics] val compute = Item(
    prefix :+ "compute",
    summary = "Time spent on commitment computations.",
    description =
      """Participant nodes compute bilateral commitments at regular intervals. This metric
        |exposes the time spent on each computation. If the time to compute the metrics
        |starts to exceed the commitment intervals, this likely indicates a problem.""",
    qualification = MetricQualification.Debug,
  )
}

class CommitmentMetrics(
    histograms: CommitmentHistograms,
    metricsFactory: LabeledMetricsFactory,
) {
  import MetricsContext.Implicits.empty
  private val prefix = histograms.prefix

  val compute: Timer = metricsFactory.timer(histograms.compute.info)

  val sequencingTime: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "sequencing-time",
        summary = "Time spent in microseconds between commitment and sequencing.",
        description = """Participant nodes compute bilateral commitments at regular intervals. After a commitment
                        |has been computed it is send for sequencing. This measures the time between the end of a
                        |commitment interval and when the commitment has been sequenced. A high value indicates that
                        |the participant is lagging behind in processing messages and computing commitments or the
                        |sequencer is slow in sequencing the commitment messages.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val catchupModeEnabled: Meter = metricsFactory.meter(
    MetricInfo(
      prefix :+ "catchup-mode-enabled",
      summary = "Times the catch up mode has been activated.",
      description =
        """Participant nodes compute bilateral commitments at regular intervals. This metric
          |exposes how often catch-up mode has been activated. Catch-up mode is triggered according
          |to catch-up config and happens if the participant lags behind on computation.""",
      qualification = MetricQualification.Debug,
    )
  )

}
