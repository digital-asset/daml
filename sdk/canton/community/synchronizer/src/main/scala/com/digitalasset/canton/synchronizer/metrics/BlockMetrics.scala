// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import cats.Eval
import com.daml.metrics.api.MetricHandle.{Gauge, Histogram, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.discard.Implicits.DiscardOps

import scala.collection.concurrent.TrieMap

/** Metrics produced by the block update generator */
class BlockMetrics private[metrics] (
    parent: MetricName,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  private val acknowledgments = new TrieMap[String, Eval[Gauge[Long]]]()

  private val prefix: MetricName = parent :+ "block"

  val height: Gauge[Long] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        name = prefix :+ "height",
        summary = "Current block height processed",
        description =
          """The submission messages are processed in blocks, where each block has an increasing number.
             |The metric shows the height of the last processed block by the given sequencer node.""".stripMargin,
        qualification = MetricQualification.Traffic,
      ),
      0L,
    )(MetricsContext.Empty)

  val delay: Gauge[Long] = openTelemetryMetricsFactory.gauge(
    MetricInfo(
      prefix :+ "delay",
      summary = "The block processing delay in milliseconds, relative to wall clock",
      description =
        """Every block carries a timestamp that was assigned by the ordering service when it ordered the block.
          |This metric shows the difference between the wall clock of the sequencer node and the timestamp
          |of the last processed block. The difference will include the clock-skew and the processing latency
          |of the ordering service. If the delay is large compared to the usual latencies, clock skew can be ruled out,
          |and enough sequencers are not slow, then it means that the node is still trying to catch up reading blocks
          |from the ordering service. This can happen after having been offline for a while or if the node is
          |too slow to keep up with the block processing load.""",
      qualification = MetricQualification.Latency,
    ),
    0L,
  )(MetricsContext.Empty)

  private val labels =
    Map("member" -> "The sender of the submission request", "type" -> "Type of request")
  val blockEvents: Meter =
    openTelemetryMetricsFactory.meter(
      MetricInfo(
        name = prefix :+ "events",
        summary = "Events processed by the sequencer, tagged by type.",
        description =
          """The sequencer forwards opaque, possibly encrypted payload. However, by looking at
            |the recipient list, the type of message can still be inferred, and tagged appropriately,
            |including the sender.""".stripMargin,
        qualification = MetricQualification.Traffic,
        labelsWithDescription = labels,
      )
    )(MetricsContext.Empty)
  val blockEventBytes: Meter =
    openTelemetryMetricsFactory.meter(
      MetricInfo(
        name = prefix :+ s"event-${Histogram.Bytes}",
        summary = "Event bytes processed by the sequencer, tagged by type.",
        description = "Similar to events, except measured by bytes",
        qualification = MetricQualification.Traffic,
        labelsWithDescription = labels,
      )
    )(MetricsContext.Empty)

  private val ackGaugeInfo: MetricInfo = MetricInfo(
    prefix :+ "acknowledgments_micros",
    "Acknowledgments by members in Micros",
    MetricQualification.Latency,
    labelsWithDescription = Map("member" -> "The sender of the acknowledgment"),
  )

  // The metrics documentation generation requires all metrics to be registered in the factory.
  // However, the following metric is registered on-demand during normal operation. Therefore,
  // we use this environment variable approach to guard against instantiation in production; but
  // register the metric for the documentation generation.
  if (sys.env.contains("GENERATE_METRICS_FOR_DOCS")) {
    openTelemetryMetricsFactory.gauge(ackGaugeInfo, 0L)(MetricsContext.Empty).discard
  }

  def updateAcknowledgementGauge(member: String, value: Long): Unit =
    acknowledgments
      .getOrElseUpdate(
        member,
        Eval.later(
          openTelemetryMetricsFactory.gauge(ackGaugeInfo, value)(
            MetricsContext("member" -> member)
          )
        ),
      )
      .value
      .updateValue(value)

}
