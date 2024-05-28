// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

class IndexedUpdatesMetrics(prefix: MetricName, metricFactory: LabeledMetricsFactory) {

  import MetricsContext.Implicits.empty

  val meteredEventsMeter: MetricHandle.Meter = metricFactory.meter(
    MetricInfo(
      prefix :+ "metered_events",
      summary = "Number of ledger events that are metered.",
      description = """Represents the number of events that will be included in the metering report.
                      |This is an estimate of the total number and not a substitute for the metering report.""",
      qualification = MetricQualification.Debug,
      labelsWithDescription = Map(
        "participant_id" -> "The id of the participant.",
        "application_id" -> "The application generating the events.",
      ),
    )
  )

  val eventsMeter: MetricHandle.Meter =
    metricFactory.meter(
      MetricInfo(
        prefix :+ "events",
        summary = "Number of ledger events processed.",
        description =
          "Represents the total number of ledger events processed (transactions, package uploads, party allocations, etc.).",
        qualification = MetricQualification.Debug,
        labelsWithDescription = Map(
          "participant_id" -> "The id of the participant.",
          "application_id" -> "The application generating the events.",
          "event_type" -> "The type of ledger event processed (transaction, reassignment, package_upload, party_allocation).",
          "status" -> "Indicates if the event was accepted or not. Possible values accepted|rejected.",
        ),
      )
    )

}

object IndexedUpdatesMetrics {

  object Labels {
    val applicationId = "application_id"
    val grpcCode = "grpc_code"
    object eventType {

      val key = "event_type"

      val partyAllocation = "party_allocation"
      val packageUpload = "package_upload"
      val transaction = "transaction"
      val reassignment = "reassignment"
    }

    object status {
      val key = "status"

      val accepted = "accepted"
      val rejected = "rejected"
    }

  }
}
