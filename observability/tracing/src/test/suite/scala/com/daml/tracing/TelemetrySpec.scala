// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tracing

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.util.Try

class TelemetrySpec extends AsyncWordSpec with TelemetrySpecBase with Matchers {
  import TelemetrySpec._
  import TelemetrySpecBase._

  "contextFromGrpcThreadLocalContext" should {
    "return a context" in {
      val span = anEmptySpan()
      span.setAttribute("existingKey", "existingValue")
      val scope = span.makeCurrent()
      try {
        val context = DefaultTelemetry.contextFromGrpcThreadLocalContext()
        context.setAttribute(anApplicationIdSpanAttribute._1, anApplicationIdSpanAttribute._2)
      } finally {
        scope.close()
        span.end()
      }

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
      val span = anEmptySpan()
      val metadata = DefaultTelemetryContext(tracer, span).encodeMetadata()

      val context = DefaultTelemetry.contextFromMetadata(Some(metadata))

      metadata.keySet contains "traceparent"
      context.encodeMetadata() shouldBe metadata
    }

    "return a root context if no metadata was provided" in {
      val context = DefaultTelemetry.contextFromMetadata(None)
      context.encodeMetadata() shouldBe empty
    }

    "return a no-op context" in {
      NoOpTelemetry.contextFromMetadata(None) shouldBe NoOpTelemetryContext
    }
  }

  "contextFromOpenTelemetryContext" should {
    "return a raw Open Telemetry Context" in {
      val span = anEmptySpan()
      val openTelemetryContext = Context.current.`with`(span)

      val context = DefaultTelemetry.contextFromOpenTelemetryContext(openTelemetryContext)

      Span.fromContextOrNull(context.openTelemetryContext) shouldBe span
    }

    "return a root context" in {
      val openTelemetryContext = Context.root

      val context = DefaultTelemetry.contextFromOpenTelemetryContext(openTelemetryContext)

      Span.fromContextOrNull(context.openTelemetryContext) shouldBe Span.getInvalid
    }

    "return a no-op context" in {
      NoOpTelemetry.contextFromOpenTelemetryContext(Context.current) shouldBe NoOpTelemetryContext
    }
  }

  "runInSpan" should {
    "create and finish a span" in {
      TestTelemetry
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

    "record an exception" in {
      Try(
        TestTelemetry
          .runInSpan(
            aSpanName,
            SpanKind.Internal,
            anApplicationIdSpanAttribute,
          ) { _ =>
            throw anException
          }
      )
      val spanAttributes = spanExporter.finishedSpanAttributes
      spanAttributes should contain(anApplicationIdSpanAttribute)

      assertExceptionRecorded
    }
  }

  "runFutureInSpan" should {
    "create and finish a span" in {
      TestTelemetry
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

    "record an exception" in {
      TestTelemetry
        .runFutureInSpan(
          aSpanName,
          SpanKind.Internal,
          anApplicationIdSpanAttribute,
        ) { _ =>
          Future.failed(anException)
        }
        .recover { case _ =>
          val spanAttributes = spanExporter.finishedSpanAttributes
          spanAttributes should contain(anApplicationIdSpanAttribute)

          assertExceptionRecorded
        }
    }
  }

  private def assertExceptionRecorded: Assertion = {
    val evenAttributes = spanExporter.finishedEventAttributes
    evenAttributes should contain(
      SpanAttribute(SemanticAttributes.EXCEPTION_TYPE) -> anExceptionName
    )
    evenAttributes should contain(
      SpanAttribute(SemanticAttributes.EXCEPTION_MESSAGE) -> anExceptionMessage
    )
  }
}

object TelemetrySpec {
  private val anExceptionMessage = "anException"
  private val anException = new IllegalStateException(anExceptionMessage)
  private val anExceptionName = anException.getClass.getCanonicalName
}
