// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Gauge, MetricsFactory}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

import java.time.Instant
import scala.annotation.nowarn

class IndexerMetrics(
    prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") factory: MetricsFactory,
) {

  @MetricDoc.Tag(
    summary = "The time of the last event ingested by the index db (in milliseconds since EPOCH).",
    description = """The last received record time is a monotonically increasing integer
                    |value that represents the record time of the last event ingested by the index
                    |db. It is measured in milliseconds since the EPOCH time.""",
    qualification = Debug,
  )
  @nowarn("cat=deprecation")
  val lastReceivedRecordTime: Gauge[Long] =
    factory.gauge(prefix :+ "last_received_record_time", 0L)(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary = "The sequential id of the current ledger end kept in the database.",
    description = """The ledger end's sequential id is a monotonically increasing integer value
                    |representing the sequential id ascribed to the most recent ledger event
                    |ingested by the index db. Please note, that only a subset of all ledger events
                    |are ingested and given a sequential id. These are: creates, consuming
                    |exercises, non-consuming exercises and divulgence events. This value can be
                    |treated as a counter of all such events visible to a given participant. This
                    |metric exposes the latest ledger end's sequential id registered in the
                    |database.""",
    qualification = Debug,
  )
  @nowarn("cat=deprecation")
  val ledgerEndSequentialId: Gauge[Long] =
    factory.gauge(prefix :+ "ledger_end_sequential_id", 0L)(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary =
      "The lag between the record time of a transaction and the wall-clock time registered at the ingestion phase to the index db (in milliseconds).",
    description = """Depending on the systemic clock skew between different machines, this value
                    |can be negative.""",
    qualification = Debug,
  )
  @nowarn("cat=deprecation")
  val currentRecordTimeLag: Gauge.CloseableGauge = factory.gaugeWithSupplier(
    prefix :+ "current_record_time_lag",
    () => Instant.now().toEpochMilli - lastReceivedRecordTime.getValue,
  )(MetricsContext.Empty)
}
