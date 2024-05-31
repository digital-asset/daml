// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}

object CommonMockMetrics {

  private val prefix = MetricName("test")

  object sequencerClient
      extends SequencerClientMetrics(
        new SequencerClientHistograms(prefix)(new HistogramInventory()),
        NoOpMetricsFactory,
      )(MetricsContext.Empty)
  object dbStorage
      extends DbStorageMetrics(
        new DbStorageHistograms(prefix)(new HistogramInventory()),
        NoOpMetricsFactory,
      )(MetricsContext.Empty)

}
