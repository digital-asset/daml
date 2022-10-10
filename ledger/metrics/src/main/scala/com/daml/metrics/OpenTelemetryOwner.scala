// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.MetricsReporter.Prometheus
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.exporter.prometheus.PrometheusCollector
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=deprecated")
case class OpenTelemetryOwner(enabled: Boolean, reporter: Option[MetricsReporter])
    extends ResourceOwner[OpenTelemetry] {

  override def acquire()(implicit
      context: ResourceContext
  ): Resource[OpenTelemetry] = {
    val meterProviderBuilder = SdkMeterProvider.builder()
    val meterProvider = if (enabled && reporter.exists(_.isInstanceOf[Prometheus])) {
      meterProviderBuilder.registerMetricReader(PrometheusCollector.create()).build()
    } else meterProviderBuilder.build()
    // register the sdk telemetry globally
    val openTelemetry =
      OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal()
    Resource(
      Future(
        openTelemetry
      )
    ) { _ =>
      Future {
        meterProvider.close()
      }
    }
  }

}
