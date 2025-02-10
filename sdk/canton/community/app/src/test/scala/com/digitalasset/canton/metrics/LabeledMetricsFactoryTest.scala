// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricQualification, MetricsInfoFilter}
import com.digitalasset.canton.BaseTest
import io.opentelemetry.api.OpenTelemetry
import org.scalatest.wordspec.AnyWordSpec

class LabeledMetricsFactoryTest extends AnyWordSpec with BaseTest {

  "metrics factory" should {
    "generate valid documentation" in {
      val inventory = new HistogramInventory()
      val histograms = new CantonHistograms()(inventory)
      val mf = MetricsRegistry(
        OpenTelemetry.noop().getMeter("test"),
        MetricsFactoryType.InMemory(_ => new InMemoryMetricsFactory),
        testingSupportAdhocMetrics = false,
        histograms,
        new MetricsInfoFilter(
          Seq.empty,
          MetricQualification.All.toSet,
        ),
        loggerFactory,
      )
      val (participantMetrics, sequencerMetrics, mediatorMetrics) = mf.metricsDoc()
      sequencerMetrics should not be empty
      mediatorMetrics should not be empty
      participantMetrics should not be empty
    }
  }

}
