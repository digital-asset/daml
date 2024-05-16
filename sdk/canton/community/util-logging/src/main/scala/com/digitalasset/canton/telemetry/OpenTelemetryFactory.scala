// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.daml.metrics.{MetricsFilter, MetricsFilterConfig}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.metrics.{HistogramInventory, OpenTelemetryOnDemandMetricsReader}
import com.digitalasset.canton.telemetry.HistogramDefinition.AggregationType
import com.digitalasset.canton.tracing.{NoopSpanExporter, TraceContext, TracingConfig}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.{
  Aggregation,
  InstrumentSelector,
  InstrumentType,
  SdkMeterProvider,
  SdkMeterProviderBuilder,
  View,
}
import io.opentelemetry.sdk.trace.`export`.{
  BatchSpanProcessor,
  BatchSpanProcessorBuilder,
  SpanExporter,
}
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.sdk.trace.{SdkTracerProvider, SdkTracerProviderBuilder}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.chaining.scalaUtilChainingOps

// TODO(#17917) Move to daml repository
final case class HistogramDefinition(
    name: String,
    aggregation: AggregationType,
    viewName: Option[String] = None,
) {

  private lazy val hasWildcards = name.contains("*") || name.contains("?")
  private lazy val asRegex = name.replace("*", ".*").replace("?", ".")

  def matches(metric: MetricName): Boolean = {
    if (hasWildcards) {
      metric.matches(asRegex)
    } else { metric.toString == name }
  }
}

object HistogramDefinition {
  sealed trait AggregationType
  final case class Buckets(boundaries: Seq[Double]) extends AggregationType
  final case class Exponential(maxBuckets: Int, maxScale: Int) extends AggregationType
}

// TODO(#17917) move to daml repository and adapt QualificationFilteringMetricsFactory
//   and ensure we add a MetricQualification.All variable
class MetricsInfoFilter(
    val filters: Seq[MetricsFilterConfig], // move back to non val when moved to daml repository
    val qualifications: Set[MetricQualification],
) {

  private val nameFilter = new MetricsFilter(filters)

  def includeMetric(info: MetricInfo): Boolean = {
    nameFilter.includeMetric(info.name.toString()) && qualifications.contains(info.qualification)
  }

}

object OpenTelemetryFactory {

  def initializeOpenTelemetry(
      initializeGlobalOpenTelemetry: Boolean,
      testingSupportAdhocMetrics: Boolean,
      metricsEnabled: Boolean,
      attachReporters: SdkMeterProviderBuilder => SdkMeterProviderBuilder,
      config: TracingConfig.Tracer,
      histogramInventory: HistogramInventory,
      histogramFilter: MetricsInfoFilter,
      histogramConfigs: Seq[HistogramDefinition],
      loggerFactory: NamedLoggerFactory,
  ): ConfiguredOpenTelemetry = {
    val logger: TracedLogger = loggerFactory.getTracedLogger(getClass)

    logger.info(
      s"Initializing open telemetry with trace-exporter=${config.exporter}, metrics-enabled=${metricsEnabled}"
    )(
      TraceContext.empty
    )
    val onDemandMetricReader = new OpenTelemetryOnDemandMetricsReader

    val exporter = createExporter(config.exporter)
    val sampler = createSampler(config.sampler)

    def setBatchSize(
        batchSize: Option[Int]
    ): BatchSpanProcessorBuilder => BatchSpanProcessorBuilder =
      builder => batchSize.fold(builder)(builder.setMaxExportBatchSize)

    def setScheduleDelay(
        scheduleDelay: Option[FiniteDuration]
    ): BatchSpanProcessorBuilder => BatchSpanProcessorBuilder = builder =>
      scheduleDelay.fold(builder)(_.toJava pipe builder.setScheduleDelay)

    val tracerProviderBuilder: SdkTracerProviderBuilder = SdkTracerProvider.builder
      .addSpanProcessor(
        BatchSpanProcessor
          .builder(exporter)
          .pipe(setBatchSize(config.batchSpanProcessor.batchSize))
          .pipe(setScheduleDelay(config.batchSpanProcessor.scheduleDelay))
          .build
      )
      .setSampler(sampler)

    def setMetricsReader: SdkMeterProviderBuilder => SdkMeterProviderBuilder = builder =>
      if (metricsEnabled) builder.registerMetricReader(onDemandMetricReader).pipe(attachReporters)
      else builder

    val meterProviderBuilder =
      addViewsToProvider(
        SdkMeterProvider.builder,
        testingSupportAdhocMetrics,
        histogramInventory,
        histogramFilter,
        histogramConfigs,
      )
        .pipe(setMetricsReader)

    val configuredSdk = OpenTelemetrySdk.builder
      .setTracerProvider(tracerProviderBuilder.build)
      .setMeterProvider(meterProviderBuilder.build)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .pipe(builder =>
        if (initializeGlobalOpenTelemetry)
          builder.buildAndRegisterGlobal
        else
          builder.build
      )

    ConfiguredOpenTelemetry(
      openTelemetry = configuredSdk,
      tracerProviderBuilder = tracerProviderBuilder,
      onDemandMetricsReader =
        if (metricsEnabled) onDemandMetricReader else NoOpOnDemandMetricsReader$,
      metricsEnabled = metricsEnabled,
    )

  }

  private def createExporter(config: TracingConfig.Exporter): SpanExporter = config match {
    case TracingConfig.Exporter.Zipkin(address, port) =>
      val httpUrl = s"http://$address:$port/api/v2/spans"
      ZipkinSpanExporter.builder.setEndpoint(httpUrl).build
    case TracingConfig.Exporter.Otlp(address, port) =>
      val httpUrl = s"http://$address:$port"
      OtlpGrpcSpanExporter.builder.setEndpoint(httpUrl).build
    case TracingConfig.Exporter.Disabled =>
      NoopSpanExporter
  }

  private def createSampler(config: TracingConfig.Sampler): Sampler = {
    val sampler = config match {
      case TracingConfig.Sampler.AlwaysOn(_) =>
        Sampler.alwaysOn()
      case TracingConfig.Sampler.AlwaysOff(_) =>
        Sampler.alwaysOff()
      case TracingConfig.Sampler.TraceIdRatio(ratio, _) =>
        Sampler.traceIdRatioBased(ratio)
    }
    if (config.parentBased) Sampler.parentBased(sampler) else sampler
  }

  // TODO(#17917) move to daml repository
  def addViewsToProvider(
      builder: SdkMeterProviderBuilder,
      testingSupportAdhocMetrics: Boolean,
      histogramInventory: HistogramInventory,
      histogramFilter: MetricsInfoFilter,
      histogramConfigs: Seq[HistogramDefinition],
  ): SdkMeterProviderBuilder = {
    val timeHistograms = (
      s"*${OpenTelemetryTimer.TimerUnitAndSuffix}",
      HistogramDefinition.Buckets(
        Seq(
          0.01d, 0.025d, 0.050d, 0.075d, 0.1d, 0.15d, 0.2d, 0.25d, 0.35d, 0.5d, 0.75d, 1d, 2.5d, 5d,
          10d,
        )
      ),
    )
    val byteHistograms = (
      s"*${Histogram.Bytes}",
      HistogramDefinition.Buckets(
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
    if (testingSupportAdhocMetrics || histogramConfigs.isEmpty) {
      // If there are no custom histogram configurations, register our default timers and byte histograms
      Seq(timeHistograms, byteHistograms).foldLeft(builder) { case (builder, (name, aggregation)) =>
        builder.registerView(
          histogramSelectorByName(name),
          explicitHistogramBucketsView(name, aggregation),
        )
      }
    } else {
      // due to https://opentelemetry.io/docs/specs/otel/metrics/sdk/#measurement-processing
      // we need to have exactly one view definition per instrument name and not multiple
      // otherwise you get lots of error messages dumped into the logs
      val configs = histogramConfigs ++ Seq(timeHistograms, byteHistograms).map {
        case (name, buckets) => HistogramDefinition(name, buckets)
      }
      // for each known histogram, register a view
      histogramInventory
        .registered()
        .filter(item => histogramFilter.includeMetric(item.info))
        .foldLeft(builder) { case (builder, item) =>
          // find the histogram config that matches the name
          val config = configs
            .find(_.matches(item.name))
            .getOrElse(HistogramDefinition(item.name, HistogramDefinition.Buckets(Seq.empty)))
          builder.registerView(
            histogramSelectorByName(item.name),
            explicitHistogramBucketsView(item.summary, config.aggregation),
          )
        }
    }
  }

  private def histogramSelectorByName(stringWithWildcards: String) = InstrumentSelector
    .builder()
    .setType(InstrumentType.HISTOGRAM)
    .setName(stringWithWildcards)
    .build()

  private def explicitHistogramBucketsView(
      description: String,
      aggregationType: AggregationType,
  ) = {
    val aggregation = aggregationType match {
      case HistogramDefinition.Buckets(buckets) =>
        if (buckets.nonEmpty)
          Aggregation.explicitBucketHistogram(
            buckets.map(Double.box).asJava
          )
        else Aggregation.explicitBucketHistogram()
      case HistogramDefinition.Exponential(maxBuckets, maxScale) =>
        Aggregation.base2ExponentialBucketHistogram(maxBuckets, maxScale)
    }
    View
      .builder()
      .setAggregation(aggregation)
      .setDescription(description)
      .build()
  }

  private def kilobytes(value: Int): Double = value * 1024d

  private def megabytes(value: Int): Double = value * 1024d * 1024d

}
