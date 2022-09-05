// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import com.daml.telemetry.TelemetrySpecBase._
import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.jdk.CollectionConverters._

trait TelemetrySpecBase extends BeforeAndAfterEach { self: Suite =>
  protected val spanExporter: InMemorySpanExporter = InMemorySpanExporter.create
  protected val tracer: Tracer = {
    val tracerProvider = SdkTracerProvider
      .builder()
      .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
      .build()
    val anInstrumentationName = this.getClass.getCanonicalName
    tracerProvider.get(anInstrumentationName)
  }

  override protected def afterEach(): Unit = {
    spanExporter.reset()
    super.afterEach()
  }

  protected def anEmptySpan(): Span = tracer.spanBuilder(aSpanName).startSpan()

  protected object TestTelemetry extends DefaultTelemetry(tracer)

  protected implicit class RichInMemorySpanExporter(exporter: InMemorySpanExporter) {
    def finishedSpanAttributes: Map[SpanAttribute, String] = finishedSpansToAttributes { spanData =>
      spanData.getAttributes.asMap.asScala.map { case (key, value) =>
        SpanAttribute(key.toString) -> value.toString
      }.toMap
    }

    def finishedEventAttributes: Map[SpanAttribute, String] = finishedSpansToAttributes {
      spanData =>
        spanData.getEvents.asScala
          .flatMap(_.getAttributes.asMap.entrySet.asScala)
          .map(entry => SpanAttribute(entry.getKey.toString) -> entry.getValue.toString)
          .toMap
    }

    private def finishedSpansToAttributes(
        spanDataToAttributes: SpanData => Map[SpanAttribute, String]
    ): Map[SpanAttribute, String] = {
      val finishedSpans = exporter.getFinishedSpanItems.asScala
      finishedSpans.flatMap(spanDataToAttributes).toMap
    }
  }
}

object TelemetrySpecBase {
  val aSpanName = "aSpan"
  val anApplicationIdSpanAttribute: (SpanAttribute, String) =
    SpanAttribute.ApplicationId -> "anApplicationId"
  val aCommandIdSpanAttribute: (SpanAttribute, String) =
    SpanAttribute.CommandId -> "aCommandId"
}
