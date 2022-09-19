// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.tracing

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.{BatchSpanProcessor, SimpleSpanProcessor, SpanExporter}
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.sdk.trace.{SdkTracerProvider, SdkTracerProviderBuilder}
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes

import java.util
import java.util.concurrent.TimeUnit

/** Provides tracer for span reporting and takes care of closing resources
  */
trait TracerProvider extends AutoCloseable {
  def tracer: Tracer
  def openTelemetry: OpenTelemetry
}

/** Generates traces and reports using given exporter
  */
private[tracing] class ReportingTracerProvider(
    exporter: SpanExporter,
    name: String,
    attributes: Map[String, String] = Map(),
) extends TracerProviderWithBuilder(
      SdkTracerProvider.builder
        .addSpanProcessor(SimpleSpanProcessor.create(exporter)),
      name,
      attributes,
    )

private[tracing] class TracerProviderWithBuilder(
    builder: SdkTracerProviderBuilder,
    name: String,
    attributes: Map[String, String] = Map(),
) extends TracerProvider {
  private val tracerProvider = {
    val attrs = attributes
      .foldRight(Attributes.builder()) { case ((key, value), builder) =>
        builder.put(s"json-api.$key", value)
      }
      .put(ResourceAttributes.SERVICE_NAME, name)
      .build()
    val serviceNameResource = Resource.create(attrs)
    builder
      .setResource(Resource.getDefault.merge(serviceNameResource))
      .build
  }

  override val openTelemetry: OpenTelemetry =
    OpenTelemetrySdk.builder
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .setTracerProvider(tracerProvider)
      .build

  override val tracer: Tracer = openTelemetry.getTracer(getClass.getName)

  override def close(): Unit = tracerProvider.close()
}

/** Generates traces but does not report
  */
object NoReportingTracerProvider extends ReportingTracerProvider(NoopSpanExporter, "no-reporting") {
  override def close(): Unit = ()
}

private object NoopSpanExporter extends SpanExporter {
  override def `export`(spans: util.Collection[SpanData]): CompletableResultCode =
    CompletableResultCode.ofSuccess()
  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
}

object TracerProvider {
  object Factory {
    def apply(config: TracingConfig.Tracer, name: String): TracerProvider = {
      val exporter = createExporter(config.exporter)
      val sampler = createSampler(config.sampler)
      val builder =
        // important to use batch span processor instead of simple span processor here because otherwise problems appear
        // with spans that are created inside grpc interceptors
        SdkTracerProvider.builder
          .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
          .setSampler(sampler)
      new TracerProviderWithBuilder(builder, name)
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
}
