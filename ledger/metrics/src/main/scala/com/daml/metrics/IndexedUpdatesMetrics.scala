// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricDoc, MetricHandle, MetricName}

class IndexedUpdatesMetrics(prefix: MetricName, metricFactory: Factory) {

  @MetricDoc.Tag(
    summary = "Number of events that will be metered",
    description = """Represents the number of events that will be included in the metering report.
        |This is an estimate of the total number and not a substitute for the metering report.""",
    qualification = Debug,
  )
  val meteredEventsMeter: MetricHandle.Meter = metricFactory.meter(
    prefix :+ "metered_events",
    "Number of events that will be metered.",
  )

}

object IndexedUpdatesMetrics {

  object Labels {

    val applicationId = "application_id"

  }
}
