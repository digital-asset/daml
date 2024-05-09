// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.telemetry.MetricsInfoFilter
import io.opentelemetry.api.OpenTelemetry
import org.scalatest.wordspec.AnyWordSpec

class LabeledMetricsFactoryTest extends AnyWordSpec with BaseTest {

  "metrics factory" should {
    // TODO(#17917) renable this test once the metrics docs have been re-enabled
    "generate valid documentation" ignore {
      val inventory = new HistogramInventory()
      val histograms = new CantonHistograms()(inventory)
      val mf = MetricsRegistry(
        OpenTelemetry.noop().getMeter("test"),
        MetricsFactoryType.InMemory(_ => new InMemoryMetricsFactory),
        testingSupportAdhocMetrics = false,
        histograms,
        new MetricsInfoFilter(
          Seq.empty,
          Set( // TODO(#17917) use MetricQualification.All once added
            MetricQualification.Errors,
            MetricQualification.Debug,
            MetricQualification.Latency,
            MetricQualification.Traffic,
            MetricQualification.Saturation,
          ),
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
