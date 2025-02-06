// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.Eval
import com.daml.metrics.api.*
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.noop.NoOpMetricsFactory

import scala.collection.concurrent.TrieMap

object TrafficConsumptionMetrics {
  val noop = new TrafficConsumptionMetrics(
    MetricName("noop"),
    NoOpMetricsFactory,
  )
}

class TrafficConsumptionMetrics(
    prefix: MetricName,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  val trafficCostOfSubmittedEvent = labeledMetricsFactory.meter(
    MetricInfo(
      prefix :+ "submitted-event-cost",
      summary = "Cost of event submitted from the sequencer client.",
      description = """When the sequencer client sends an event to the sequencer to be sequenced,
           it will record on this metric the cost of the event. Note that the event may or may not end up being sequenced.
           So this metric may not exactly match the actual consumed traffic.""",
      qualification = MetricQualification.Traffic,
    )
  )(MetricsContext.Empty)

  val trafficCostOfDeliveredSequencedEvent = labeledMetricsFactory.meter(
    MetricInfo(
      prefix :+ "event-delivered-cost",
      summary = "Cost of events that were sequenced and delivered.",
      description = """Cost of events for which the sender received confirmation that they were delivered.
          There is an exception for aggregated submissions: the cost of aggregate events will be recorded
          as soon as the event is ordered and the sequencer waits to receive threshold-many events.
          The final event may or may not be delivered successfully depending on the result of the aggregation.
          """,
      qualification = MetricQualification.Traffic,
    )
  )(MetricsContext.Empty)

  val deliveredEventCounter: Counter = labeledMetricsFactory.counter(
    MetricInfo(
      prefix :+ "event-delivered",
      summary = "Number of events that were sequenced and delivered.",
      description = """Counter for event-delivered-cost.""",
      qualification = MetricQualification.Traffic,
    )
  )

  val trafficCostOfNotDeliveredSequencedEvent = labeledMetricsFactory.meter(
    MetricInfo(
      prefix :+ "event-rejected-cost",
      summary = "Cost of events that were sequenced but no delivered successfully.",
      description =
        """Cost of events for which the sender received confirmation that the events will not be delivered.
           The reason for non-delivery is labeled on the metric, if available.
          """,
      qualification = MetricQualification.Traffic,
    )
  )(MetricsContext.Empty)

  val rejectedEventCounter: Counter = labeledMetricsFactory.counter(
    MetricInfo(
      prefix :+ "event-rejected",
      summary = "Number of events that were sequenced but not delivered.",
      description = """Counter for event-rejected-cost.""",
      qualification = MetricQualification.Traffic,
    )
  )

  // Gauges don't support metrics context per update. So instead create a map with a gauge per context.
  private val lastTrafficUpdateTimestampMetrics: TrieMap[MetricsContext, Eval[Gauge[Long]]] =
    TrieMap.empty[MetricsContext, Eval[Gauge[Long]]]

  def lastTrafficUpdateTimestamp(context: MetricsContext): Gauge[Long] = {
    def createLastTrafficUpdateTimestampGauge: Gauge[Long] =
      labeledMetricsFactory.gauge[Long](
        MetricInfo(
          prefix :+ "last-traffic-update",
          summary = "Timestamp of the last event that update the traffic state of a member.",
          description =
            """When a member sends or receives an event, its traffic state is updated. This metrics is the timestamp (seconds since epoch) of the last update received.""",
          qualification = MetricQualification.Traffic,
        ),
        0L,
      )(context)

    // Eval.later guards against concurrent creation of the same gauge. For traffic updates specifically this should not happen
    // as of the time of writing this though because updates should be made sequentially from the BUG, but this is an extra precaution.
    lastTrafficUpdateTimestampMetrics
      .getOrElseUpdate(context, Eval.later(createLastTrafficUpdateTimestampGauge))
      .value
  }

  // Gauges don't support metrics context per update. So instead create a map with a gauge per context.
  private val extraTrafficPurchasedMetrics: TrieMap[MetricsContext, Eval[Gauge[Long]]] =
    TrieMap.empty[MetricsContext, Eval[Gauge[Long]]]

  def extraTrafficPurchased(context: MetricsContext): Gauge[Long] = {
    def createExtraTrafficPurchasedGauge: Gauge[Long] =
      labeledMetricsFactory.gauge[Long](
        MetricInfo(
          prefix :+ "extra-traffic-purchased",
          summary = "Extra traffic purchased.",
          description = """Total extra traffic purchased.""",
          qualification = MetricQualification.Traffic,
        ),
        0L,
      )(context)

    // Eval.later guards against concurrent creation of the same gauge. For traffic updates specifically this should not happen
    // as of the time of writing this though because updates should be made sequentially from the BUG, but this is an extra precaution.
    extraTrafficPurchasedMetrics
      .getOrElseUpdate(context, Eval.later(createExtraTrafficPurchasedGauge))
      .value
  }

  // Gauges don't support metrics context per update. So instead create a map with a gauge per context.
  private val extraTrafficConsumedMetrics: TrieMap[MetricsContext, Eval[Gauge[Long]]] =
    TrieMap.empty[MetricsContext, Eval[Gauge[Long]]]

  def extraTrafficConsumed(context: MetricsContext): Gauge[Long] = {
    def createExtraTrafficConsumedGauge: Gauge[Long] =
      labeledMetricsFactory.gauge[Long](
        MetricInfo(
          prefix :+ "extra-traffic-consumed",
          summary = "Extra traffic consumed.",
          description = """Total extra traffic consumed.""",
          qualification = MetricQualification.Traffic,
        ),
        0L,
      )(context)

    // Eval.later guards against concurrent creation of the same gauge. For traffic updates specifically this should not happen
    // as of the time of writing this though because updates should be made sequentially from the BUG, but this is an extra precaution.
    extraTrafficConsumedMetrics
      .getOrElseUpdate(context, Eval.later(createExtraTrafficConsumedGauge))
      .value
  }

  // Gauges don't support metrics context per update. So instead create a map with a gauge per context.
  private val baseTrafficRemainderMetrics: TrieMap[MetricsContext, Eval[Gauge[Long]]] =
    TrieMap.empty[MetricsContext, Eval[Gauge[Long]]]

  def baseTrafficRemainder(context: MetricsContext): Gauge[Long] = {
    def createBaseTrafficRemainderGauge: Gauge[Long] =
      labeledMetricsFactory.gauge[Long](
        MetricInfo(
          prefix :+ "base-traffic-remainder",
          summary = "Base traffic remainder",
          description = """Base traffic remainder available.""",
          qualification = MetricQualification.Traffic,
        ),
        0L,
      )(context)

    // Eval.later guards against concurrent creation of the same gauge. For traffic updates specifically this should not happen
    // as of the time of writing this though because updates should be made sequentially from the BUG, but this is an extra precaution.
    baseTrafficRemainderMetrics
      .getOrElseUpdate(context, Eval.later(createBaseTrafficRemainderGauge))
      .value
  }

}
