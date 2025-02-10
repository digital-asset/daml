// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class DeclarativeApiMetrics(val base: MetricName, factory: LabeledMetricsFactory)(implicit
    metricsContext: MetricsContext
) {
  private val prefix: MetricName = base :+ "declarative_api"

  val items: Gauge[Int] = factory.gauge(
    MetricInfo(
      prefix :+ "items",
      "Number of items managed through the declarative API",
      MetricQualification.Debug,
      "This metric indicates the number of items managed through the declarative API",
    ),
    0,
  )

  val errors: Gauge[Int] = factory.gauge(
    MetricInfo(
      prefix :+ "errors",
      "Errors for the last update",
      MetricQualification.Errors,
      """The node will attempt to apply the changes configured in the declarative config file.
         A positive number means that some items failed to be synchronised. A negative number
         means that the overall synchronisation procedure failed with an error. :
         0 = everything good, -1 = config file unreadable, -2 = context could not be created,
         -3 = failure while applying items, -9 = exception caught.""",
    ),
    0,
  )

}
