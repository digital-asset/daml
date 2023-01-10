// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.MetricName

abstract class DatabaseMetricsFactory(prefix: MetricName, factory: Factory) {

  protected def createDbMetrics(name: String): DatabaseMetrics = {
    new DatabaseMetrics(prefix, name, factory)
  }
}
