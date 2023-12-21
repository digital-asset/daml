// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory as DamlNoOpMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.api.{MetricHandle as DamlMetricHandle, MetricName, MetricsContext}
import io.opentelemetry.api.metrics

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration

object MetricHandle {

  @deprecated("Use LabeledMetricsFactory to create metrics that support labels.", since = "2.7.0")
  trait MetricsFactory extends DamlMetricHandle.MetricsFactory {

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

  @nowarn("cat=deprecation")
  trait LabeledMetricsFactory extends MetricsFactory with DamlMetricHandle.LabeledMetricsFactory

  @nowarn("cat=deprecation")
  class CantonDropwizardMetricsFactory(registry: MetricRegistry)
      extends DropwizardMetricsFactory(registry)
      with MetricsFactory

  object NoOpMetricsFactory extends DamlNoOpMetricsFactory with LabeledMetricsFactory

  class CantonOpenTelemetryMetricsFactory(
      otelMeter: metrics.Meter,
      globalMetricsContext: MetricsContext,
  ) extends OpenTelemetryMetricsFactory(otelMeter, globalMetricsContext)
      with LabeledMetricsFactory

}
