// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.tracing.SpanAttribute
import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.data.{EventData, SpanData}
import io.opentelemetry.sdk.trace.`export`.SpanExporter

import java.util
import java.util.concurrent.LinkedBlockingQueue
import scala.jdk.CollectionConverters.MapHasAsScala

class TestTelemetrySetup() extends AutoCloseable {
  private lazy val testExporter = new TestTelemetry.TestExporter()
  private val tracerProvider = new ReportingTracerProvider(testExporter, "test")

  val tracer: Tracer = tracerProvider.tracer
  def reportedSpans(): List[SpanData] = testExporter.allSpans()

  def reportedSpanAttributes: Map[SpanAttribute, String] =
    reportedSpans()
      .flatMap({ spanData =>
        spanData.getAttributes.asMap.asScala.map { case (key, value) =>
          SpanAttribute(key.toString) -> value.toString
        }.toMap
      })
      .toMap

  def anEmptySpan(spanName: String = "aSpan"): Span =
    tracer.spanBuilder(spanName).startSpan()

  override def close(): Unit = testExporter.close()
}

object TestTelemetry {

  class TestExporter extends SpanExporter {
    val queue = new LinkedBlockingQueue[SpanData]
    override def `export`(spans: util.Collection[SpanData]): CompletableResultCode = {
      queue.addAll(spans)
      CompletableResultCode.ofSuccess()
    }
    def allSpans(): List[SpanData] = {
      import scala.jdk.CollectionConverters.*
      queue.iterator().asScala.toList.reverse
    }
    override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
    override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
  }

  def eventsOrderedByTime(spans: SpanData*): List[EventData] = {
    import scala.jdk.CollectionConverters.*
    spans.toList
      .flatMap(_.getEvents.asScala)
      .map(ev => (ev, ev.getEpochNanos))
      .sortBy(_._2)
      .map(_._1)
  }
}
