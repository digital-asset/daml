// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.metrics.HistogramDefinition
import com.daml.telemetry.OpenTelemetryOwner.addViewsToProvider
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.metrics.OpenTelemetryOnDemandMetricsReader
import com.digitalasset.canton.tracing.{NoopSpanExporter, TraceContext, TracingConfig}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.{SdkMeterProvider, SdkMeterProviderBuilder}
import io.opentelemetry.sdk.trace.`export`.{
  BatchSpanProcessor,
  BatchSpanProcessorBuilder,
  SpanExporter,
}
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.sdk.trace.{SdkTracerProvider, SdkTracerProviderBuilder}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.chaining.scalaUtilChainingOps

object OpenTelemetryFactory {

  def initializeOpenTelemetry(
      initializeGlobalOpenTelemetry: Boolean,
      metricsEnabled: Boolean,
      attachReporters: SdkMeterProviderBuilder => SdkMeterProviderBuilder,
      config: TracingConfig.Tracer,
      histograms: Seq[HistogramDefinition],
      loggerFactory: NamedLoggerFactory,
  ): ConfiguredOpenTelemetry = {
    val logger: TracedLogger = loggerFactory.getTracedLogger(getClass)
    logger.info(s"Initializing open telemetry with Exporter.${config.exporter}")(
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

    val meterProviderBuilder = addViewsToProvider(SdkMeterProvider.builder, histograms)
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
    case TracingConfig.Exporter.Jaeger(address, port) =>
      JaegerGrpcSpanExporter.builder
        .setEndpoint(s"http://$address:$port")
        .setTimeout(30, TimeUnit.SECONDS)
        .build
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

}
