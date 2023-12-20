// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.{SimpleSpanProcessor, SpanExporter}
import io.opentelemetry.semconv.ResourceAttributes

import java.util

/** Provides tracer for span reporting and takes care of closing resources
 */
trait TracerProvider {
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
  ConfiguredOpenTelemetry(
    OpenTelemetrySdk
      .builder()
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .build(),
    SdkTracerProvider.builder
      .addSpanProcessor(SimpleSpanProcessor.create(exporter)),
    NoOpOnDemandMetricsReader$,
  ),
  name,
  attributes,
)

private[tracing] class TracerProviderWithBuilder(
                                                  configuredOpenTelemetry: ConfiguredOpenTelemetry,
                                                  name: String,
                                                  attributes: Map[String, String] = Map(),
                                                ) extends TracerProvider {
  private val tracerProvider: SdkTracerProvider = {
    val attrs = attributes
      .foldRight(Attributes.builder()) { case ((key, value), builder) =>
        builder.put(s"canton.$key", value)
      }
      .put(ResourceAttributes.SERVICE_NAME, name)
      .build()
    val serviceNameResource = Resource.create(attrs)
    configuredOpenTelemetry.tracerProviderBuilder
      .setResource(Resource.getDefault.merge(serviceNameResource))
      .build
  }

  override val openTelemetry: OpenTelemetry =
    OpenTelemetrySdk.builder
      .setPropagators(configuredOpenTelemetry.openTelemetry.getPropagators)
      .setMeterProvider(configuredOpenTelemetry.openTelemetry.getSdkMeterProvider)
      .setLoggerProvider(configuredOpenTelemetry.openTelemetry.getSdkLoggerProvider)
      .setTracerProvider(tracerProvider)
      .build

  override val tracer: Tracer = tracerProvider.tracerBuilder(getClass.getName).build()

}

/** Generates traces but does not report
 */
object NoReportingTracerProvider extends ReportingTracerProvider(NoopSpanExporter, "no-reporting")

object NoopSpanExporter extends SpanExporter {
  override def `export`(spans: util.Collection[SpanData]): CompletableResultCode =
    CompletableResultCode.ofSuccess()

  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
}

object TracerProvider {
  object Factory {
    def apply(configuredOpenTelemetry: ConfiguredOpenTelemetry, name: String): TracerProvider =
      new TracerProviderWithBuilder(configuredOpenTelemetry, name)

  }
}
