// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import cats.data.EitherT
import com.codahale.{metrics => codahale}

import scala.concurrent.{Future, blocking}

sealed trait MetricHandle[T <: codahale.Metric] {
  def name: String
  def metric: T
  def metricType: String // type string used for documentation purposes
}

object MetricHandle {

  trait Factory {

    def prefix: MetricName

    def registry: codahale.MetricRegistry

    def timer(name: MetricName): Timer = Timer(name, registry.timer(name))

    def varGauge[T](name: MetricName, initial: T): Gauge[VarGauge[T], T] =
      addGauge(name, VarGauge[T](initial), _.updateValue(initial))

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    private def addGauge[T <: codahale.Gauge[M], M](
        name: MetricName,
        newGauge: => T,
        resetExisting: (T => Unit),
    ): Gauge[T, M] = blocking {
      synchronized {
        val res: Gauge[T, M] = Option(registry.getGauges.get(name: String)) match {
          case Some(existingGauge) => Gauge(name, existingGauge.asInstanceOf[T])
          case None =>
            val gauge = newGauge
            // This is not idempotent, therefore we need to query first.
            registry.register(name, gauge)
            Gauge(name, gauge)
        }
        resetExisting(res.metric)
        res
      }
    }

    def meter(name: MetricName): Meter = {
      // This is idempotent
      Meter(name, registry.meter(name))
    }

    def counter(name: MetricName): Counter = {
      // This is idempotent
      Counter(name, registry.counter(name))
    }

    def histogram(name: MetricName): Histogram = {
      Histogram(name, registry.histogram(name))
    }

  }

  trait FactoryWithDBMetrics extends MetricHandle.Factory {
    def createDbMetrics(name: String): DatabaseMetrics =
      new DatabaseMetrics(prefix, name, registry)
  }

  sealed case class Timer(name: String, metric: codahale.Timer) extends MetricHandle[codahale.Timer] {
    def metricType: String = "Timer"

    def timeEitherT[E, A](ev: EitherT[Future, E, A]): EitherT[Future, E, A] = {
      EitherT(Timed.future(metric, ev.value))
    }

  }

  sealed case class Gauge[U <: codahale.Gauge[T], T](name: String, metric: U)
      extends MetricHandle[codahale.Gauge[T]] {
    def metricType: String = "Gauge"
  }

  sealed case class Meter(name: String, metric: codahale.Meter) extends MetricHandle[codahale.Meter] {
    def metricType: String = "Meter"
  }

  sealed case class Counter(name: String, metric: codahale.Counter) extends MetricHandle[codahale.Counter] {
    def metricType: String = "Counter"
  }

  sealed case class Histogram(name: String, metric: codahale.Histogram)
      extends MetricHandle[codahale.Histogram] {
    def metricType: String = "Histogram"
  }

  type VarGaugeM[T] = Gauge[VarGauge[T], T]

}
