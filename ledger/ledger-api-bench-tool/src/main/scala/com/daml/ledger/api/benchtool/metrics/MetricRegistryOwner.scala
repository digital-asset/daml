// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.util.concurrent.TimeUnit

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.api.opentelemetry.Slf4jMetricExporter
import com.daml.metrics.api.reporters.MetricsReporter
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.PeriodicMetricReader
import org.slf4j.Logger

import scala.concurrent.duration.Duration

class MetricRegistryOwner(
    reporter: MetricsReporter,
    reportingInterval: Duration,
    logger: Logger,
) extends ResourceOwner[MeterProvider] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[MeterProvider] =
    ResourceOwner.forCloseable(() => metricOwner).acquire()

  private def metricOwner = {
    val loggingMetricReader = PeriodicMetricReader
      .builder(new Slf4jMetricExporter(logger))
      .setInterval(reportingInterval.toMillis, TimeUnit.MILLISECONDS)
      .build()
    val meterProviderBuilder = SdkMeterProvider
      .builder()
    reporter match {
      case MetricsReporter.Console => meterProviderBuilder.registerMetricReader(loggingMetricReader)
      case MetricsReporter.Prometheus(address) =>
        meterProviderBuilder
          .registerMetricReader(loggingMetricReader)
          .registerMetricReader(
            PrometheusHttpServer
              .builder()
              .setHost(address.getHostString)
              .setPort(address.getPort)
              .build()
          )
      case _ => throw new IllegalArgumentException(s"Metric reporter $reporter not supported.")
    }
    meterProviderBuilder.build()
  }

}
