// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.dropwizard

import com.codahale.{metrics => codahale}
import com.daml.metrics.api.Gauges.VarGauge
import com.daml.metrics.api.MetricHandle.{Counter, MetricsFactory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{Gauges, MetricName, MetricsContext}
import com.daml.scalautil.Statement.discard

import scala.concurrent.blocking

class DropwizardMetricsFactory(val registry: codahale.MetricRegistry) extends MetricsFactory {

  override def timer(name: MetricName, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Timer = DropwizardTimer(name, registry.timer(name))

  override def gauge[T](name: MetricName, initial: T, description: String = "")(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Gauge[T] = {
    val registeredGauge = reRegisterGauge[T, VarGauge[T]](name, Gauges.VarGauge(initial))
    DropwizardGauge(name, registeredGauge)
  }

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => T,
      description: String = "",
  )(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Unit = discard {
    reRegisterGauge[T, AsyncGauge[T]](
      name,
      AsyncGauge(gaugeSupplier),
    )
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
