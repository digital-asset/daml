// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.noop

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.MetricHandle.Gauge.ClosableGauge
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}

trait NoOpMetricsFactory extends Factory {

  override def timer(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Timer = NoOpTimer(name)

  override def gauge[T](
      name: MetricName,
      initial: T,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Gauge[T] = NoOpGauge(name, initial)

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String,
  )(implicit context: MetricsContext): ClosableGauge = () => ()

  override def meter(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Meter = NoOpMeter(name)

  override def counter(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Counter = NoOpCounter(name)

  override def histogram(
      name: MetricName,
      description: String,
  )(implicit
      context: MetricsContext
  ): MetricHandle.Histogram = NoOpHistogram(name)
}

object NoOpMetricsFactory extends NoOpMetricsFactory
