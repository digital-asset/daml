// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.util.concurrent.TimeUnit

import com.daml.metrics.api.opentelemetry.{
  OpenTelemetryMetricsFactory,
  OpenTelemetryUtil,
  Slf4jMetricExporter,
}
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.PeriodicMetricReader

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class MetricsOwner[Context: HasExecutionContext](interval: Duration)
    extends AbstractResourceOwner[Context, Metrics] {

  override def acquire()(implicit
      context: Context
  ): Resource[Context, Metrics] = {
    ReleasableResource(
      Future.successful(
        SdkMeterProvider
          .builder()
          .registerMetricReader(
            PeriodicMetricReader
              .builder(new Slf4jMetricExporter())
              .setInterval(interval.toMillis, TimeUnit.MILLISECONDS)
              .newMetricReaderFactory()
          )
          .build()
      )
    )(meterProvider => OpenTelemetryUtil.completionCodeToFuture(meterProvider.shutdown()))
      .map(meterProvider =>
        new Metrics(new OpenTelemetryMetricsFactory(meterProvider.get("non-repudiation")))
      )
  }
}
