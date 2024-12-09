// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}

object SequencerTestMetrics
    extends SequencerMetrics(
      new SequencerHistograms(MetricName("test"))(new HistogramInventory),
      NoOpMetricsFactory,
    )

object MediatorTestMetrics
    extends MediatorMetrics(
      new MediatorHistograms(MetricName("test"))(new HistogramInventory),
      NoOpMetricsFactory,
    )
