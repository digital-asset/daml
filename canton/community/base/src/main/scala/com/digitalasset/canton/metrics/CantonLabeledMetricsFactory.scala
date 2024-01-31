// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.noop.NoOpMetricsFactory as DamlNoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.api.{MetricHandle as DamlMetricHandle, MetricName, MetricsContext}
import io.opentelemetry.api.metrics

import scala.concurrent.duration.FiniteDuration

trait CantonLabeledMetricsFactory extends DamlMetricHandle.LabeledMetricsFactory {

  def loadGauge(
      name: MetricName,
      interval: FiniteDuration,
      timer: Timer,
  )(implicit mc: MetricsContext): TimedLoadGauge = {
    val definedLoadGauge = new LoadGauge(interval)
    val registeredLoadGauge = gaugeWithSupplier(name, () => definedLoadGauge.getLoad)
    new TimedLoadGauge(timer, definedLoadGauge, registeredLoadGauge)
  }

}

object CantonLabeledMetricsFactory {

  trait Generator {
    def create(context: MetricsContext): CantonLabeledMetricsFactory
  }

  object NoOpMetricsFactory extends DamlNoOpMetricsFactory with CantonLabeledMetricsFactory

  class CantonOpenTelemetryMetricsFactory(
      otelMeter: metrics.Meter,
      globalMetricsContext: MetricsContext,
  ) extends OpenTelemetryMetricsFactory(otelMeter, globalMetricsContext)
      with CantonLabeledMetricsFactory

}
