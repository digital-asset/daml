// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.dropwizard

import com.codahale.{metrics => codahale}
import com.daml.metrics.api.Gauges.VarGauge
import com.daml.metrics.api.MetricHandle.Gauge.{CloseableGauge, SimpleCloseableGauge}
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, Meter, MetricsFactory, Timer}
import com.daml.metrics.api.{Gauges, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

import scala.annotation.nowarn
import scala.concurrent.blocking

@nowarn("cat=deprecation")
class DropwizardMetricsFactory(val registry: codahale.MetricRegistry) extends MetricsFactory {

  override def timer(name: MetricName, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Timer = DropwizardTimer(name, registry.timer(name))

  override def gauge[T](name: MetricName, initial: T, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Gauge[T] = {
    val registeredGauge = reRegisterGauge[T, VarGauge[T]](name, Gauges.VarGauge(initial))
    DropwizardGauge(name, registeredGauge, () => discard(registry.remove(name)))
  }

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String = "",
  )(implicit
      context: MetricsContext = MetricsContext.Empty
  ): CloseableGauge = {
    reRegisterGauge[T, AsyncGauge[T]](
      name,
      AsyncGauge(gaugeSupplier),
    )
    SimpleCloseableGauge(name, () => discard(registry.remove(name)))
  }

  override def meter(name: MetricName, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Meter = {
    // This is idempotent
    DropwizardMeter(name, registry.meter(name))
  }

  override def counter(name: MetricName, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Counter = {
    // This is idempotent
    DropwizardCounter(name, registry.counter(name))
  }

  override def histogram(name: MetricName, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Histogram = {
    DropwizardHistogram(name, registry.histogram(name))
  }

  protected def reRegisterGauge[T, G <: codahale.Gauge[T]](
      name: MetricName,
      gauge: G,
  ): G = blocking {
    synchronized {
      registry.remove(name)
      registry.register(name, gauge)
    }
  }
}
