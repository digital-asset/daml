// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Other cases are covered by [[TelemetrySpec]] */
class TelemetryContextSpec extends AnyWordSpec with TelemetrySpecBase with Matchers {
  import TelemetrySpecBase._

  "DefaultTelemetryContext.runInOpenTelemetryScope" should {
    "run a body and create a current context with a span" in {
      val span = anEmptySpan()
      span.setAttribute(
        anApplicationIdSpanAttribute._1.key,
        anApplicationIdSpanAttribute._2,
      )

      runInOpenTelemetryScopeAndAssert(DefaultTelemetryContext(tracer, span))

      val attributes = spanExporter.finishedSpanAttributes
      attributes should contain(anApplicationIdSpanAttribute)
    }

    "return a raw Open Telemetry context" in {
      val span = anEmptySpan()

      val openTelemetryContext = DefaultTelemetryContext(tracer, span).openTelemetryContext

      Span.fromContext(openTelemetryContext) shouldBe span
    }
  }

  "RootDefaultTelemetryContext.runInOpenTelemetryScope" should {
    "run a body" in {
      runInOpenTelemetryScopeAndAssert(RootDefaultTelemetryContext(tracer))
    }
  }

  "NoOpTelemetryContext.runInOpenTelemetryScope" should {
    "run a body" in {
      runInOpenTelemetryScopeAndAssert(NoOpTelemetryContext)
    }
  }

  private def runInOpenTelemetryScopeAndAssert(telemetryContext: TelemetryContext): Assertion = {
    var placeholder: Option[_] = None
    telemetryContext.runInOpenTelemetryScope {
      Span
        .fromContext(Context.current())
        .end() // end the span from the current context to be able to make assertions on its attributes
      placeholder = Some(())
    }
    placeholder shouldBe Some(())
  }
}
