// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class PruningMetrics(
    val prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
) {

  import MetricsContext.Implicits.empty

  object commitments {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "commitments")

    val compute: Timer = metricsFactory.timer(
      MetricInfo(
        prefix :+ "compute",
        summary = "Time spent on commitment computations.",
        description =
          """Participant nodes compute bilateral commitments at regular intervals. This metric
          |exposes the time spent on each computation. If the time to compute the metrics
          |starts to exceed the commitment intervals, this likely indicates a problem.""",
        qualification = MetricQualification.Debug,
      )
    )

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

  object prune {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "prune")

    val overall: Timer = metricsFactory.timer(
      MetricInfo(
        prefix,
        summary = "Duration of prune operations.",
        description =
          """This timer exposes the duration of pruning requests from the Canton portion of the ledger.""",
        qualification = MetricQualification.Saturation,
      )
    )

    val maxEventAge: Gauge[Long] =
      metricsFactory.gauge[Long](
        MetricInfo(
          prefix :+ "max-event-age",
          summary = "Age of oldest unpruned event.",
          description =
            """This gauge exposes the age of the oldest, unpruned event in hours as a way to quantify the
            |pruning backlog.""",
          qualification = MetricQualification.Saturation,
        ),
        0L,
      )(MetricsContext.Empty)

  }
}
