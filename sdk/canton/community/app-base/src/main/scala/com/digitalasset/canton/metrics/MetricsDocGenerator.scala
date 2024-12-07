// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricsContext}

import scala.collection.mutable.ListBuffer

/** Fake labelled factory used to collect metrics */
class MetricsDocGenerator extends LabeledMetricsFactory {

  private val noop = NoOpMetricsFactory

  def getAll(): List[MetricDoc.Item] = assembled.toList.map { case (metricType, info) =>
    MetricDoc.Item(
      name = info.name.toString(),
      summary = info.summary,
      description = info.description.stripMargin,
      metricType = metricType,
      qualification = info.qualification,
      labelsWithDescription = info.labelsWithDescription,
    )
  }

  def reset(): Unit = assembled.clear()

  private val assembled = new ListBuffer[(String, MetricInfo)]()

  override def timer(info: MetricInfo)(implicit context: MetricsContext): MetricHandle.Timer = {
    assembled.addOne(("timer", info))
    noop.timer(info)
  }

  override def gauge[T](info: MetricInfo, initial: T)(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = {
    assembled.addOne(("gauge", info))
    noop.gauge(info, initial)
  }

  override def gaugeWithSupplier[T](info: MetricInfo, gaugeSupplier: () => T)(implicit
      context: MetricsContext
  ): Gauge.CloseableGauge = {
    assembled.addOne(("gauge", info))
    noop.gaugeWithSupplier(info, gaugeSupplier)
  }

  override def meter(info: MetricInfo)(implicit context: MetricsContext): MetricHandle.Meter = {
    assembled.addOne(("meter", info))
    noop.meter(info)
  }

  override def counter(info: MetricInfo)(implicit context: MetricsContext): MetricHandle.Counter = {
    assembled.addOne(("counter", info))
    noop.counter(info)
  }

  override def histogram(info: MetricInfo)(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = {
    assembled.addOne(("histogram", info))
    noop.histogram(info)
  }
}
