// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.metrics.api.{HistogramInventory, MetricsInfoFilter}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TracingConfig}
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{SpanContext, SpanKind, Tracer}
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo
import io.opentelemetry.sdk.trace.ReadableSpan
import io.opentelemetry.sdk.trace.data.SpanData
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@nowarn("cat=deprecation")
class UnsetSpanEndingThreadReferenceSpanProcessorTest extends AnyWordSpec with BaseTest {

  "UnsetSpanEndingThreadReference" should {
    "unset the spanEndingThread field after the span has ended" in {
      val configuredTracerProvider = OpenTelemetryFactory
        .initializeOpenTelemetry(
          initializeGlobalOpenTelemetry = false,
          testingSupportAdhocMetrics = false,
          metricsEnabled = false,
          attachReporters = identity,
          config = TracingConfig.Tracer(),
          histogramInventory = new HistogramInventory,
          histogramFilter = new MetricsInfoFilter(Seq.empty, Set.empty),
          histogramConfigs = Seq.empty,
          cardinality = 1,
          loggerFactory,
        )
        .openTelemetry
        .getTracerProvider
        .get(getClass.getSimpleName)

      val noReportingTracer = NoReportingTracerProvider.tracer

      def createSpanAndAssert(tracer: Tracer) = {
        // start a span
        val span = tracer.spanBuilder("test").startSpan()
        // and end it immediately
        span.end()

        // SdkSpan is not publicly accessible
        span.getClass shouldBe UnsetSpanEndingThreadReferenceSpanProcessor.sdkSpanClass
        UnsetSpanEndingThreadReferenceSpanProcessor.spanEndingThreadField.get(span) should be(null)
      }

      createSpanAndAssert(configuredTracerProvider)
      createSpanAndAssert(noReportingTracer)
    }

    "only log an error once" in {
      val spanProcessor = new UnsetSpanEndingThreadReferenceSpanProcessor(loggerFactory)

      def createDummySpan(): ReadableSpan = new ReadableSpan {
        override def getSpanContext: SpanContext = ???
        override def getParentSpanContext: SpanContext = ???
        override def getName: String = ???
        override def toSpanData: SpanData = ???
        override def getInstrumentationLibraryInfo: InstrumentationLibraryInfo = ???
        override def hasEnded: Boolean = ???
        override def getLatencyNanos: Long = ???
        override def getKind: SpanKind = ???
        override def getAttribute[T](key: AttributeKey[T]): T = ???
      }

      spanProcessor.hasErrorBeenLogged.get shouldBe false

      // call onEnd multiple times with a dummy span. this should neither trigger an error log nor an exception,
      // because the dummy span is not an instance of SdkSpan.
      spanProcessor.onEnd(createDummySpan())
      spanProcessor.onEnd(createDummySpan())
      spanProcessor.onEnd(createDummySpan())

      loggerFactory.assertLogs(
        // calling the actual method for processing a span with a span that is not an instance of SdkSpan, should throw an exception
        spanProcessor.unsetSpanEndingThreadField(createDummySpan()),
        _.errorMessage should include("Error unsetting the spanEndingThread field for span"),
      )

      spanProcessor.hasErrorBeenLogged.get shouldBe true

      // now we can call unsetSpanEndingThreadField without suppressing the logs,
      // because the error will not get logged again.
      spanProcessor.unsetSpanEndingThreadField(createDummySpan())

    }
  }

}
