// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName

abstract class DatabaseMetricsFactory(prefix: MetricName, factory: LabeledMetricsFactory) {

  protected def createDbMetrics(name: String): DatabaseMetrics = {
    new DatabaseMetrics(prefix :+ name, factory)
  }
}
