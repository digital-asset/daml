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

  override def timer(name: MetricName): Timer = DropwizardTimer(name, registry.timer(name))

  override def gauge[T](name: MetricName, initial: T)(implicit
      context: MetricsContext = MetricsContext.Empty
  ): Gauge[T] = {
    val registeredgauge = reRegisterGauge[T, VarGauge[T]](name, Gauges.VarGauge(initial))
    DropwizardGauge(name, registeredgauge)
  }

  override def gaugeWithSupplier[T](
      name: MetricName,
      gaugeSupplier: () => () => (T, MetricsContext),
  ): Unit =
    synchronized {
      registry.remove(name)
      val _ = registry.gauge(
        name,
        () => {
          val valueGetter = gaugeSupplier()
          new codahale.Gauge[T] { override def getValue: T = valueGetter()._1 }
        },
      )
      ()
    }

  override def meter(name: MetricName): Meter = {
    // This is idempotent
    DropwizardMeter(name, registry.meter(name))
  }

  override def counter(name: MetricName): Counter = {
    // This is idempotent
    DropwizardCounter(name, registry.counter(name))
  }

  override def histogram(name: MetricName): Histogram = {
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
  def createDbMetrics(name: String): DatabaseMetrics =
    new DatabaseMetrics(prefix, name, registry)
}
