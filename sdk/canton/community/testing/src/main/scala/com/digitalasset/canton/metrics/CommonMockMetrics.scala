// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{MetricName, MetricsContext}

object CommonMockMetrics {

  private val prefix = MetricName("test")

  object sequencerClient
      extends SequencerClientMetrics(prefix, NoOpMetricsFactory)(MetricsContext.Empty)
  object dbStorage extends DbStorageMetrics(prefix, NoOpMetricsFactory)

}
