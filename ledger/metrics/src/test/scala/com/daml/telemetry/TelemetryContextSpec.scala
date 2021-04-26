// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.telemetry

import io.opentelemetry.api.trace.Span
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

/** Other cases are covered by [[TelemetrySpec]] */
class TelemetryContextSpec extends AnyWordSpecLike with Matchers {

  "DefaultTelemetryContext.runInOpenTelemetryScope" should {
    "run a body" in {
      val span = Span.current()
      runInOpenTelemetryScopeAndAssert(DefaultTelemetryContext(span))
    }
  }

  "RootDefaultTelemetryContext.runInOpenTelemetryScope" should {
    "run a body" in {
      runInOpenTelemetryScopeAndAssert(RootDefaultTelemetryContext)
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
      placeholder = Some(())
    }
    placeholder shouldBe Some(())
  }
}
