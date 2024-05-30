// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import com.daml.metrics.{ExecutorServiceMetrics, HistogramDefinition}
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.{Aggregation, InstrumentSelector, View}

import scala.jdk.CollectionConverters.SeqHasAsJava

object OpenTelemetryOwner {

  /** Add views to providers
    *
    * In open telemetry, you have to define the histogram views separately from the metric itself. Even worse,
    * you need to define it before you define the metrics. If you define two views that match to the same metrics,
    * you end up with ugly warning messages and errors: https://opentelemetry.io/docs/specs/otel/metrics/sdk/#measurement-processing
    *
    * The solution to this is to statically define all the metric names in advance separately, create appropriate views
    * and then, on each histogram definition check that an appropriate static definition exists.
    */
  def addViewsToProvider(
      builder: SdkMeterProviderBuilder,
      configs: Seq[HistogramDefinition],
      metrics: Set[MetricName],
  ): SdkMeterProviderBuilder = {

    val candidates = configs ++ Seq(
      // use smaller buckets for the executor services (must be declared before the generic timing buckets)
      HistogramDefinition(
        s"${ExecutorServiceMetrics.Prefix}*${OpenTelemetryTimer.TimerUnitAndSuffix}",
        Seq(
          0.0005d, 0.001d, 0.002d, 0.005d, 0.01d, 0.025d, 0.05d, 0.1d, 0.25d, 0.5d, 0.75d, 1d, 2.5d,
        ),
      ),
      // timing buckets for gRPC server latency measurements with more precise granularity on latencies up to 5s
      HistogramDefinition(
        s"${DamlGrpcServerMetrics.GrpcServerMetricsPrefix}*${OpenTelemetryTimer.TimerUnitAndSuffix}",
        Seq(
          0.01d, 0.025d, 0.050d, 0.075d, 0.1d, 0.15d, 0.2d, 0.25d, 0.35d, 0.5d, 0.75d, 1d, 1.25d,
          1.5d, 1.75d, 2d, 2.25d, 2.5d, 2.75d, 3d, 3.5d, 4d, 4.5d, 5d, 10d,
        ),
      ),
      // generic timing buckets
      HistogramDefinition(
        s"*${OpenTelemetryTimer.TimerUnitAndSuffix}",
        Seq(
          0.01d, 0.025d, 0.050d, 0.075d, 0.1d, 0.15d, 0.2d, 0.25d, 0.35d, 0.5d, 0.75d, 1d, 2.5d, 5d,
          10d,
        ),
      ),
      // use size specific buckets
      HistogramDefinition(
        s"*${Histogram.Bytes}",
        Seq(
          kilobytes(10),
          kilobytes(50),
          kilobytes(100),
          kilobytes(500),
          megabytes(1),
          megabytes(5),
          megabytes(10),
          megabytes(50),
        ),
      ),
    )

    metrics.foreach { metric =>
      candidates.find(_.name.matches(metric)).foreach { histogram =>
        builder.registerView(
          InstrumentSelector
            .builder()
            .setType(InstrumentType.HISTOGRAM)
            .setMeterName(metric)
            .build(),
          explicitHistogramBucketsView(histogram.bucketBoundaries),
        )
      }
      builder
    }
    builder
  }

  private def explicitHistogramBucketsView(buckets: Seq[Double]) = View
    .builder()
    .setAggregation(
      Aggregation.explicitBucketHistogram(
        buckets.map(Double.box).asJava
      )
    )
    .build()

  private def kilobytes(value: Int): Double = value * 1024d

  private def megabytes(value: Int): Double = value * 1024d * 1024d

}
