// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import cats.Eval
import com.daml.metrics.api.MetricHandle.{Gauge, Histogram, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

import scala.collection.concurrent.TrieMap

/** Metrics produced by the block update generator */
class BlockMetrics(
    parent: MetricName,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

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

  private val labels =
    Map("sender" -> "The sender of the submission request", "type" -> "Type of request")
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

  private val ackGaugeInfo = MetricInfo(prefix :+ "acknowledgments_micros", "Acknowledgments by senders in Micros", MetricQualification.Latency, labelsWithDescription = Map("sender" -> "The sender of the acknowledgment"))

  def updateAcknowledgementGauge(sender: String, value: Long): Unit =
    acknowledgments
      .getOrElseUpdate(
        sender, {
          Eval.later(
            openTelemetryMetricsFactory.gauge(ackGaugeInfo, value)(
              MetricsContext("sender" -> sender)
            )
          )
        },
      )
      .value
      .updateValue(value)

  private val acknowledgments = new TrieMap[String, Eval[Gauge[Long]]]()

}
