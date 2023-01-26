// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.MetricsFactory
import com.daml.metrics.api.{MetricDoc, MetricHandle, MetricName}

class IndexedUpdatesMetrics(prefix: MetricName, metricFactory: MetricsFactory) {

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

  @MetricDoc.Tag(
    summary = "Updates processed by the indexer",
    description = "Represents the total number of updates processed, that are sent for indexing.",
    qualification = Debug,
  )
  val eventsMeter: MetricHandle.Meter =
    metricFactory.meter(prefix :+ "events", "Number of events ingested by the indexer.")

}

object IndexedUpdatesMetrics {

  object Labels {
    val applicationId = "application_id"
    val grpcCode = "grpc_code"
    object eventType {

      val key = "event_type"

      val configurationChange = "configuration_change"
      val partyAllocation = "party_allocation"
      val packageUpload = "package_upload"
      val transaction = "transaction"
    }

    object status {
      val key = "status"

      val accepted = "accepted"
      val rejected = "rejected"
    }

  }
}
