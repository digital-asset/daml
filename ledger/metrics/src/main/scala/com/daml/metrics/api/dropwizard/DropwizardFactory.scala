// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.dropwizard

import com.codahale.{metrics => codahale}
import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.Gauges.VarGauge
import com.daml.metrics.api.MetricHandle.{Counter, Factory, Gauge, Histogram, Meter, Timer}
import com.daml.metrics.api.{Gauges, MetricName, MetricsContext}

import scala.concurrent.blocking

trait DropwizardFactory extends Factory {

  def registry: codahale.MetricRegistry

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
  ): Unit =
    synchronized {
      registry.remove(name)
      val _ = registry.gauge(
        name,
        () => {
          new codahale.Gauge[T] { override def getValue: T = gaugeSupplier() }
        },
      )
      ()
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

trait FactoryWithDBMetrics extends DropwizardFactory {

  def prefix: MetricName

  def createDbMetrics(name: String): DatabaseMetrics =
    new DatabaseMetrics(prefix, name, registry)
}
