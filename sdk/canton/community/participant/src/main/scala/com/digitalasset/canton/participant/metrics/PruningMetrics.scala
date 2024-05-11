// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.metrics.HistogramInventory
import com.digitalasset.canton.metrics.HistogramInventory.Item

class PruningHistograms(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "pruning"

  private[metrics] val overall = Item(
    prefix,
    summary = "Duration of prune operations.",
    description =
      """This timer exposes the duration of pruning requests from the Canton portion of the ledger.""",
    qualification = MetricQualification.Saturation,
  )

}

class PruningMetrics(
    histograms: PruningHistograms,
    metricsFactory: LabeledMetricsFactory,
) {

  import MetricsContext.Implicits.empty

  private val prefix = histograms.prefix

  val overall: Timer = metricsFactory.timer(histograms.overall.info)

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
