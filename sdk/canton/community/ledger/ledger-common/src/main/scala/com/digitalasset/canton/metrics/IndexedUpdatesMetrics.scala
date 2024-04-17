// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricQualification.Debug
import com.daml.metrics.api.{MetricDoc, MetricHandle, MetricName}

class IndexedUpdatesMetrics(prefix: MetricName, metricFactory: LabeledMetricsFactory) {

  @MetricDoc.Tag(
    summary = "Number of ledger events that are metered.",
    description = """Represents the number of events that will be included in the metering report.
        |This is an estimate of the total number and not a substitute for the metering report.""",
    qualification = Debug,
    labelsWithDescription = Map(
      "participant_id" -> "The id of the participant.",
      "application_id" -> "The application generating the events.",
    ),
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
    labelsWithDescription = Map(
      "participant_id" -> "The id of the participant.",
      "application_id" -> "The application generating the events.",
      "event_type" -> "The type of ledger event processed (transaction, package upload, party allocation, configuration change).",
      "status" -> "Indicates if the transaction was accepted or not. Possible values accepted|rejected.",
    ),
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
