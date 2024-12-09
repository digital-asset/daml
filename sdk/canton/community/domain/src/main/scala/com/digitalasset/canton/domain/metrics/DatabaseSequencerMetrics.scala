// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.api.*
import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.metrics.HasDocumentedMetrics

class DatabaseSequencerMetrics(
    parent: MetricName,
    metricsFactory: MetricHandle.LabeledMetricsFactory,
) extends HasDocumentedMetrics {
  private val prefix: MetricName = parent :+ "db"

  val watermarkDelay: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "watermark_delay",
      summary = "The event processing delay in milliseconds, relative to wall clock",
      description = """Sequencer writes events in parallel using a watermark.
          |This metric shows the difference between the wall clock of the sequencer node and the current watermark
          |of the last written events. The difference will include the clock-skew and the processing latency
          |of the sequencer database write.
          |For block sequencers if the delay is large compared to the usual latencies, clock skew can be ruled out,
          |and enough sequencers are not slow, then it means that the node is still trying to catch up reading blocks
          |from the ordering service. This can happen after having been offline for a while or if the node is
          |too slow to keep up with the block processing load.
          |For database sequencers it means that database system is not being able to keep up with the write load.""",
      qualification = MetricQualification.Latency,
    ),
    0L,
  )(MetricsContext.Empty)
}
