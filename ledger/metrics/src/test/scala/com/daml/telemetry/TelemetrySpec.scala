// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class TelemetrySpec extends AsyncWordSpec with BeforeAndAfterEach with Matchers {

  import TelemetrySpec._

  override protected def afterEach(): Unit = spanExporter.reset()

  "contextFromGrpcThreadLocalContext" should {
    "return a context" in {
      tracerProvider
        .get(InstrumentationName)
        .spanBuilder(aSpanName)
        .startSpan()
        .makeCurrent()
      Span.current.setAttribute("existingKey", "existingValue")
      val context = DefaultTelemetry.contextFromGrpcThreadLocalContext()
      context.setAttribute(anApplicationIdSpanAttribute._1, anApplicationIdSpanAttribute._2)
      Span.current.end()

      val attributes = spanExporter.finishedSpanAttributes
      attributes should contain(SpanAttribute("existingKey") -> "existingValue")
      attributes should contain(anApplicationIdSpanAttribute)
    }

    "return a no-op context" in {
      NoOpTelemetry.contextFromGrpcThreadLocalContext() shouldBe NoOpTelemetryContext
    }
  }

  "contextFromMetadata" should {
    "return an extracted context" in {
      val span = tracerProvider
        .get(InstrumentationName)
        .spanBuilder(aSpanName)
        .startSpan()
      val metadata = DefaultTelemetryContext(span).encodeMetadata()

      val context = DefaultTelemetry.contextFromMetadata(Some(metadata))

      metadata.keySet contains "traceparent"
      context.encodeMetadata() shouldBe metadata
    }

    "return a root context" in {
      val context = DefaultTelemetry.contextFromMetadata(None)
      context.encodeMetadata() shouldBe empty
    }

    "return a no-op context" in {
      NoOpTelemetry.contextFromMetadata(None) shouldBe NoOpTelemetryContext
    }
  }

  "runInSpan" should {
    "create and finish a span" in {
      DefaultTelemetry
        .runInSpan(
          aSpanName,
          SpanKind.Internal,
          anApplicationIdSpanAttribute,
        ) { telemetryContext =>
          telemetryContext.setAttribute(aCommandIdSpanAttribute._1, aCommandIdSpanAttribute._2)
        }

      val attributes = spanExporter.finishedSpanAttributes
      attributes should contain(anApplicationIdSpanAttribute)
      attributes should contain(aCommandIdSpanAttribute)
    }
  }

  "runFutureInSpan" should {
    "create and finish a span" in {
      DefaultTelemetry
        .runFutureInSpan(
          aSpanName,
          SpanKind.Internal,
          anApplicationIdSpanAttribute,
        ) { telemetryContext =>
          telemetryContext.setAttribute(aCommandIdSpanAttribute._1, aCommandIdSpanAttribute._2)
          Future.unit
        }
        .map { _ =>
          val attributes = spanExporter.finishedSpanAttributes
          attributes should contain(anApplicationIdSpanAttribute)
          attributes should contain(aCommandIdSpanAttribute)
        }
    }
  }
}

object TelemetrySpec {
  private val aSpanName = "aSpan"
  private val anApplicationIdSpanAttribute: (SpanAttribute, String) =
    SpanAttribute.ApplicationId -> "anApplicationId"
  private val aCommandIdSpanAttribute: (SpanAttribute, String) =
    SpanAttribute.CommandId -> "aCommandId"

  private val spanExporter: InMemorySpanExporter = InMemorySpanExporter.create
  private val tracerProvider: SdkTracerProvider = SdkTracerProvider
    .builder()
    .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
    .build()

  GlobalOpenTelemetry.set(
    OpenTelemetrySdk
      .builder()
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .setTracerProvider(tracerProvider)
      .build()
  )

  private implicit class RichInMemorySpanExporter(exporter: InMemorySpanExporter) {
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
