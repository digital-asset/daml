// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.NoOpMetricsFactory

object SequencerTestMetrics
    extends SequencerMetrics(
      MetricName("test"),
      NoOpMetricsFactory,
      new DamlGrpcServerMetrics(NoOpMetricsFactory, "test"),
      new HealthMetrics(NoOpMetricsFactory),
    )

object MediatorTestMetrics
    extends MediatorMetrics(
      MetricName("test"),
      NoOpMetricsFactory,
      new DamlGrpcServerMetrics(NoOpMetricsFactory, "test"),
      new HealthMetrics(NoOpMetricsFactory),
    )
