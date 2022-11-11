// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.OpenTelemetryMeterOwner.buildProviderWithViews
import com.daml.metrics.api.reporters.MetricsReporter
import com.daml.metrics.api.reporters.MetricsReporter.Prometheus
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.exporter.prometheus.PrometheusCollector
import io.opentelemetry.sdk.metrics.common.InstrumentType
import io.opentelemetry.sdk.metrics.view.{Aggregation, InstrumentSelector, View}
import io.opentelemetry.sdk.metrics.{SdkMeterProvider, SdkMeterProviderBuilder}

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.jdk.CollectionConverters.SeqHasAsJava

@nowarn("msg=deprecated")
case class OpenTelemetryMeterOwner(enabled: Boolean, reporter: Option[MetricsReporter])
    extends ResourceOwner[Meter] {

  override def acquire()(implicit
      context: ResourceContext
  ): Resource[Meter] = {
    val meterProviderBuilder = buildProviderWithViews

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
object OpenTelemetryMeterOwner {

  def buildProviderWithViews: SdkMeterProviderBuilder =
    SdkMeterProvider
      .builder()
      .registerView(
        InstrumentSelector
          .builder()
          .setType(InstrumentType.HISTOGRAM)
          .setName((t: String) => t.endsWith("duration.ms"))
          .build(),
        View
          .builder()
          .setAggregation(
            Aggregation.explicitBucketHistogram(
              Seq(5d, 10d, 15d, 25d, 35d, 50d, 75d, 100d, 150d, 200d, 250d, 350d, 500d, 750d,
                1_000d, 2_500d, 5_000d, 7_500d, 10_000d, 25_000d, 50_000d).map(Double.box).asJava
            )
          )
          .build(),
      )
}
