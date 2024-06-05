// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import cats.Eval
import com.daml.metrics.api.MetricDoc.MetricQualification
import com.daml.metrics.api.MetricHandle.{Gauge, Histogram, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

import scala.collection.concurrent.TrieMap

/** Metrics produced by the block update generator */
class BlockMetrics(
    parent: MetricName,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  private val prefix: MetricName = parent :+ "block"

  @MetricDoc.Tag(
    summary = "Current block height processed",
    description =
      """The submission messages are processed in blocks, where each block has an increasing number.
                    |The metric shows the height of the last processed block by the given sequencer node.""",
    qualification = MetricQualification.Traffic,
  )
  val height: Gauge[Long] =
    openTelemetryMetricsFactory.gauge(
      name = prefix :+ "height",
      0L,
      description = "Current block height processed",
    )(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary = "Events processed by the sequencer, tagged by type and sender",
    description =
      """The sequencer forwards opaque, possibly encrypted payload. However, by looking at
        |the recipient list, the type of message can still be inferred, and tagged appropriately,
        |including the sender.""",
    qualification = MetricQualification.Traffic,
  )
  val blockEvents: Meter =
    openTelemetryMetricsFactory.meter(
      name = prefix :+ "events"
    )(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary = "Event bytes processed by the sequencer, tagged by type and sender.",
    description = """Similar to events, except measured by bytes.""",
    qualification = MetricQualification.Traffic,
  )
  val blockEventBytes: Meter =
    openTelemetryMetricsFactory.meter(
      prefix :+ s"event-${Histogram.Bytes}"
    )(MetricsContext.Empty)

  def updateAcknowledgementGauge(sender: String, value: Long): Unit =
    acknowledgments
      .getOrElseUpdate(
        sender, {
          Eval.later(
            openTelemetryMetricsFactory.gauge(prefix :+ "acknowledgments_micros", value)(
              MetricsContext("sender" -> sender)
            )
          )
        },
      )
      .value
      .updateValue(value)

  private val acknowledgments = new TrieMap[String, Eval[Gauge[Long]]]()

}
