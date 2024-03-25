// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricName
import com.digitalasset.canton.metrics.MetricHandle.NoOpMetricsFactory

object CommonMockMetrics {

  private val prefix = MetricName("test")

  object sequencerClient extends SequencerClientMetrics(prefix, NoOpMetricsFactory)
  object dbStorage extends DbStorageMetrics(prefix, NoOpMetricsFactory)

}
