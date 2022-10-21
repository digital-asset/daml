// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.MetricsReporter.Prometheus
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.exporter.prometheus.PrometheusCollector
import io.opentelemetry.sdk.metrics.SdkMeterProvider

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=deprecated")
case class OpenTelemetryMeterOwner(enabled: Boolean, reporter: Option[MetricsReporter])
    extends ResourceOwner[Meter] {

  override def acquire()(implicit
      context: ResourceContext
  ): Resource[Meter] = {
    val meterProviderBuilder = SdkMeterProvider.builder()

    /* To integrate with prometheus we're using the deprecated [[PrometheusCollector]].
     * More details about the deprecation here: https://github.com/open-telemetry/opentelemetry-java/issues/4284
     * This forces us to keep the current opentelemetry version (see ticket for paths forward)
     */
    val meterProvider = if (enabled && reporter.exists(_.isInstanceOf[Prometheus])) {
      meterProviderBuilder.registerMetricReader(PrometheusCollector.create()).build()
    } else meterProviderBuilder.build()
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
