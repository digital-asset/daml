package com.daml.metrics

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.MetricName

abstract class DatabaseMetricsFactory(prefix: MetricName, factory: Factory) {

  protected def createDbMetrics(name: String): DatabaseMetrics = {
    new DatabaseMetrics(prefix, name, factory)
  }
}
