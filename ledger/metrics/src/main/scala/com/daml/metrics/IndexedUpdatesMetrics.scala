// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.MetricsFactory
import com.daml.metrics.api.{MetricDoc, MetricHandle, MetricName}

class IndexedUpdatesMetrics(prefix: MetricName, metricFactory: MetricsFactory) {

  @MetricDoc.Tag(
    summary = "Number of ledger events that are metered.",
    description = """Represents the number of events that will be included in the metering report.
        |This is an estimate of the total number and not a substitute for the metering report.""",
    qualification = Debug,
  )
  val meteredEventsMeter: MetricHandle.Meter = metricFactory.meter(
    prefix :+ "metered_events",
    """Represents the number of events that will be included in the metering report.
        |This is an estimate of the total number and not a substitute for the metering report.""".stripMargin,
  )

  @MetricDoc.Tag(
    summary = "Number of transactions processed.",
    description =
      "Represents the total number of transaction acceptance, transaction rejection, package upload, party allocation, etc. events processed.",
    qualification = Debug,
  )
  val eventsMeter: MetricHandle.Meter =
    metricFactory.meter(
      prefix :+ "events",
      "Represents the total number of transaction acceptance, transaction rejection, package upload, party allocation, etc. processed.",
    )

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
