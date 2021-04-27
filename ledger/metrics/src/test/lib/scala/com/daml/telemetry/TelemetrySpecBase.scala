// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor

import scala.jdk.CollectionConverters._

trait TelemetrySpecBase {

  protected val anInstrumentationName = "com.daml.telemetry.TelemetrySpec"
  protected val aSpanName = "aSpan"
  protected val anApplicationIdSpanAttribute: (SpanAttribute, String) =
    SpanAttribute.ApplicationId -> "anApplicationId"
  protected val aCommandIdSpanAttribute: (SpanAttribute, String) =
    SpanAttribute.CommandId -> "aCommandId"

  protected val spanExporter: InMemorySpanExporter = InMemorySpanExporter.create
  protected val tracerProvider: SdkTracerProvider = SdkTracerProvider
    .builder()
    .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
    .build()

  protected object TestTelemetry extends DefaultTelemetry(tracerProvider.get(anInstrumentationName))

  protected implicit class RichInMemorySpanExporter(exporter: InMemorySpanExporter) {
    def finishedSpanAttributes: Map[SpanAttribute, String] = {
      val finishedSpans = exporter.getFinishedSpanItems.asScala
      finishedSpans.flatMap { span =>
        val attributes = span.getAttributes.asMap.asScala
        attributes.map { case (key, value) =>
          SpanAttribute(key.toString) -> value.toString
        }
      }.toMap
    }
  }
}
