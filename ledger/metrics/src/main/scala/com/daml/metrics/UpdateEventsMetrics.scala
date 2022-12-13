package com.daml.metrics

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricHandle, MetricName}

class UpdateEventsMetrics(prefix: MetricName, metricFactory: Factory) {

  val meteredEventsCounter: MetricHandle.Counter = metricFactory.counter(prefix :+ "metered_events")

}
