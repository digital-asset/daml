// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.OpenTelemetryMeterOwner.buildProviderWithViews
import com.daml.metrics.api.reporters.MetricsReporter
import com.daml.metrics.api.reporters.MetricsReporter.Prometheus
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.sdk.metrics.internal.view.ExponentialHistogramAggregation
import io.opentelemetry.sdk.metrics.{
  InstrumentSelector,
  InstrumentType,
  SdkMeterProvider,
  SdkMeterProviderBuilder,
  View,
}

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=deprecated")
case class OpenTelemetryMeterOwner(enabled: Boolean, reporter: Option[MetricsReporter])
    extends ResourceOwner[Meter] {

  override def acquire()(implicit
      context: ResourceContext
  ): Resource[Meter] = {
    val meterProviderBuilder = buildProviderWithViews

    val meterProvider = reporter
      .collect {
        case prometheus: Prometheus if enabled => prometheus
      }
      .map { prometheusReporter =>
        meterProviderBuilder
          .registerMetricReader(
            PrometheusHttpServer
              .builder()
              .setHost(prometheusReporter.host)
              .setPort(prometheusReporter.port)
              .build()
          )
          .build()
      }
      .getOrElse(meterProviderBuilder.build())
    Resource(
      Future(
        meterProvider.meterBuilder("daml").build()
      )
    ) { _ =>
      Future {
        meterProvider.close()
      }
    }
  }

}

object OpenTelemetryMeterOwner {

  def buildProviderWithViews: SdkMeterProviderBuilder = {
    SdkMeterProvider
      .builder()
      .registerView(
        histogramSelector(),
        exponentialHistogramAggregation(10),
      )
  }

  private def histogramSelector() = InstrumentSelector
    .builder()
    .setType(InstrumentType.HISTOGRAM)
    .build()

  private def exponentialHistogramAggregation(buckets: Int) = View
    .builder()
    .setAggregation(
      ExponentialHistogramAggregation.create(buckets)
    )
    .build()

}
