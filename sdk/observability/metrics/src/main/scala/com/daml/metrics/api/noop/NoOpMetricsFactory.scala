// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.noop

import com.daml.metrics.api.MetricHandle.Gauge.{CloseableGauge, SimpleCloseableGauge}
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricsContext}

class NoOpMetricsFactory extends LabeledMetricsFactory {

  override def timer(info: MetricInfo)(implicit
      metricsContext: MetricsContext
  ): MetricHandle.Timer = NoOpTimer(info)

  override def gauge[T](
      info: MetricInfo,
      initial: T,
  )(implicit
      metricsContext: MetricsContext
  ): MetricHandle.Gauge[T] = NoOpGauge(info, initial)

  override def gaugeWithSupplier[T](
      info: MetricInfo,
      gaugeSupplier: () => T,
  )(implicit metricsContext: MetricsContext): CloseableGauge = SimpleCloseableGauge(info, () => ())

  override def meter(
      info: MetricInfo
  )(implicit
      metricsContext: MetricsContext
  ): MetricHandle.Meter = NoOpMeter(info)

  override def counter(
      info: MetricInfo
  )(implicit
      metricsContext: MetricsContext
  ): MetricHandle.Counter = NoOpCounter(info)

  override def histogram(
      info: MetricInfo
  )(implicit
      metricsContext: MetricsContext
  ): MetricHandle.Histogram = NoOpHistogram(info)
}

object NoOpMetricsFactory extends NoOpMetricsFactory
