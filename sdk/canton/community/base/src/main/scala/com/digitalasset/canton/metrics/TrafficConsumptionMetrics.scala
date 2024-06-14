// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.Eval
import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.MetricQualification.Traffic
import com.daml.metrics.api.*
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

  @MetricDoc.Tag(
    summary = "Extra traffic consumed.",
    description = """Total extra traffic consumed.""",
    qualification = Traffic,
  )
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
