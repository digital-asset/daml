// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.{ExecutorServiceMetrics, HistogramDefinition}
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer
import com.daml.metrics.api.reporters.MetricsReporter
import com.daml.metrics.api.reporters.MetricsReporter.Prometheus
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.telemetry.OpenTelemetryOwner.addViewsToProvider
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.exporter.prometheus.PrometheusCollector
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder
import io.opentelemetry.sdk.metrics.common.InstrumentType
import io.opentelemetry.sdk.metrics.view.{Aggregation, InstrumentSelector, View}

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.jdk.CollectionConverters.SeqHasAsJava

@nowarn("msg=deprecated")
case class OpenTelemetryOwner(
    setAsGlobal: Boolean,
    reporter: Option[MetricsReporter],
    histograms: Seq[HistogramDefinition],
) extends ResourceOwner[OpenTelemetry] {

  override def acquire()(implicit
      context: ResourceContext
  ): Resource[OpenTelemetry] = {
    Resource(
      Future {
        if (sys.props.get("otel.traces.exporter").isEmpty) {
          // if no trace exporter is configured then default to none instead of the oltp default used by the library
          sys.props.addOne("otel.traces.exporter" -> "none")
        }
        AutoConfiguredOpenTelemetrySdk
          .builder()
          .addMeterProviderCustomizer { case (builder, _) =>
            val meterProviderBuilder = addViewsToProvider(builder, histograms)
            /* To integrate with prometheus we're using the deprecated [[PrometheusCollector]].
             * More details about the deprecation here: https://github.com/open-telemetry/opentelemetry-java/issues/4284
             * This forces us to keep the current OpenTelemetry version (see ticket for potential paths forward).
             */
            if (reporter.exists(_.isInstanceOf[Prometheus])) {
              meterProviderBuilder.registerMetricReader(PrometheusCollector.create())
            } else meterProviderBuilder
          }
          .registerShutdownHook(false)
          .setResultAsGlobal(setAsGlobal)
          .build()
          .getOpenTelemetrySdk
      }
    ) { sdk =>
      Future {
        sdk.getSdkMeterProvider.close()
        sdk.getSdkTracerProvider.close()
      }
    }
  }

}

object OpenTelemetryOwner {

  def addViewsToProvider(
      builder: SdkMeterProviderBuilder,
      histograms: Seq[HistogramDefinition],
  ): SdkMeterProviderBuilder = {
    // Only one view is going to be applied, and it's in the order of it's definition
    // therefore the config views must be registered first to be able to override the code defined views
    val builderWithCustomViews = histograms.foldRight(builder) { case (histogram, builder) =>
      builder.registerView(
        histogramSelectorWithRegex(histogram.nameRegex),
        explicitHistogramBucketsView(histogram.bucketBoundaries),
      )
    }
    builderWithCustomViews
      // use smaller buckets for the executor services (must be declared before the generic timing buckets
      .registerView(
        histogramSelectorWithRegex(
          s"${ExecutorServiceMetrics.Prefix}.*${OpenTelemetryTimer.TimerUnitAndSuffix}"
        ),
        explicitHistogramBucketsView(
          Seq(
            0.0005d, 0.001d, 0.002d, 0.005d, 0.01d, 0.025d, 0.05d, 0.1d, 0.25d, 0.5d, 0.75d, 1d,
            2.5d,
          )
        ),
      )
      // timing buckets for gRPC server latency measurements with more precise granularity on latencies up to 5s
      .registerView(
        histogramSelectorWithRegex(
          s"${ExecutorServiceMetrics.GrpcServerMetricsPrefix}.*${OpenTelemetryTimer.TimerUnitAndSuffix}"
        ),
        explicitHistogramBucketsView(
          Seq(
            0.01d, 0.025d, 0.050d, 0.075d, 0.1d, 0.15d, 0.2d, 0.25d, 0.35d, 0.5d, 0.75d, 1d, 1.25d,
            1.5d, 1.75d, 2d, 2.25d, 2.5d, 2.75d, 3d, 3.25d, 3.5d, 3.75d, 4d, 4.25d, 4.5d, 4.75d, 5d,
            10d,
          )
        ),
      )
      // generic timing buckets
      .registerView(
        histogramSelectorWithRegex(s".*${OpenTelemetryTimer.TimerUnitAndSuffix}"),
        explicitHistogramBucketsView(
          Seq(
            0.01d, 0.025d, 0.050d, 0.075d, 0.1d, 0.15d, 0.2d, 0.25d, 0.35d, 0.5d, 0.75d, 1d, 2.5d,
            5d, 10d,
          )
        ),
      )
      // use size specific buckets
      .registerView(
        histogramSelectorWithRegex(s".*${Histogram.Bytes}"),
        explicitHistogramBucketsView(
          Seq(
            kilobytes(10),
            kilobytes(50),
            kilobytes(100),
            kilobytes(500),
            megabytes(1),
            megabytes(5),
            megabytes(10),
            megabytes(50),
          )
        ),
      )
  }

  private def histogramSelectorWithRegex(regex: String) = InstrumentSelector
    .builder()
    .setType(InstrumentType.HISTOGRAM)
    .setName((t: String) => t.matches(regex))
    .build()

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
