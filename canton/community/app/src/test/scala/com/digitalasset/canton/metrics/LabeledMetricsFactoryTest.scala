// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.BaseTest
import io.opentelemetry.api.OpenTelemetry
import org.scalatest.wordspec.AnyWordSpec

class LabeledMetricsFactoryTest extends AnyWordSpec with BaseTest {

  "metrics factory" should {
    "generate valid documentation" in {
      val mf = MetricsRegistry(
        OpenTelemetry.noop().getMeter("test"),
        MetricsFactoryType.InMemory(_ => new InMemoryMetricsFactory),
      )
      val (participantMetrics, sequencerMetrics, mediatorMetrics) = mf.metricsDoc()
      sequencerMetrics should not be empty
      mediatorMetrics should not be empty
      participantMetrics should not be empty
    }
  }

}
