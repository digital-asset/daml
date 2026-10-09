// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LogReporter
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.PeriodicMetricReader

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class MetricRegistryOwner(reportingInterval: Duration, loggerFactory: NamedLoggerFactory)
    extends ResourceOwner[MeterProvider] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[MeterProvider] =
    ResourceOwner.forCloseable(() => metricOwner).acquire()

  private def metricOwner = {
    val loggingMetricReader = PeriodicMetricReader
      .builder(new LogReporter(logAsInfo = true, loggerFactory))
      .setInterval(reportingInterval.toMillis, TimeUnit.MILLISECONDS)
      .build()
    val meterProviderBuilder = SdkMeterProvider
      .builder()
    meterProviderBuilder.registerMetricReader(loggingMetricReader)
    meterProviderBuilder.build()
  }

}
