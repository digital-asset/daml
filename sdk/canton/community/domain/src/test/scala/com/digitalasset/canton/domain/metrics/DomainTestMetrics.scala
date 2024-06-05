// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.digitalasset.canton.metrics.MetricHandle.NoOpMetricsFactory

object DomainTestMetrics
    extends DomainMetrics(
      MetricName("test"),
      NoOpMetricsFactory,
      NoOpMetricsFactory,
      new DamlGrpcServerMetrics(NoOpMetricsFactory, "test"),
      new HealthMetrics(NoOpMetricsFactory),
    )
