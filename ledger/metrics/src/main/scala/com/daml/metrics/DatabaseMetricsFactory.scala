// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry

final class DatabaseMetricsFactory private[metrics] (
    registry: MetricRegistry,
    prefix: MetricName,
) {

  def apply(name: String): DatabaseMetrics =
    new DatabaseMetrics(registry, prefix, name)

}
