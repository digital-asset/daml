// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Gauge, MetricsFactory, Timer}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

import scala.annotation.nowarn

class PruningMetrics(
    val prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") metricsFactory: MetricsFactory,
) {

  object commitments {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "commitments")

    @MetricDoc.Tag(
      summary = "Time spent on commitment computations.",
      description =
        """Participant nodes compute bilateral commitments at regular intervals. This metric
        |exposes the time spent on each computation. If the time to compute the metrics
        |starts to exceed the commitment intervals, this likely indicates a problem.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val compute: Timer = metricsFactory.timer(prefix :+ "compute")
  }

  object prune {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "prune")

    @MetricDoc.Tag(
      summary = "Duration of prune operations.",
      description =
        """This timer exposes the duration of pruning requests from the Canton portion of the ledger.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val overall: Timer = metricsFactory.timer(prefix)

    @MetricDoc.Tag(
      summary = "Age of oldest unpruned event.",
      description =
        """This gauge exposes the age of the oldest, unpruned event in hours as a way to quantify the
          |pruning backlog.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val maxEventAge: Gauge[Long] =
      metricsFactory.gauge[Long](MetricName(prefix :+ "max-event-age"), 0L)(MetricsContext.Empty)

  }
}
