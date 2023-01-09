// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.time.Instant

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Factory, Gauge}
import com.daml.metrics.api.noop.NoOpGauge
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

class IndexerMetrics(prefix: MetricName, factory: Factory) {

  @MetricDoc.Tag(
    summary = "The time of the last event ingested by the index db (in milliseconds since EPOCH).",
    description = """The last received record time is a monotonically increasing integer
                    |value that represents the record time of the last event ingested by the index
                    |db. It is measured in milliseconds since the EPOCH time.""",
    qualification = Debug,
  )
  val lastReceivedRecordTime: Gauge[Long] =
    factory.gauge(prefix :+ "last_received_record_time", 0L)(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary = "A string value representing the last ledger offset ingested by the index db.",
    description = """It is only available on metrics backends that support strings. In particular,
                    |it is not available in Prometheus.""",
    qualification = Debug,
  )
  val lastReceivedOffset: Gauge[String] =
    factory.gauge(prefix :+ "last_received_offset", "<none>")(MetricsContext.Empty)

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
  val ledgerEndSequentialId: Gauge[Long] =
    factory.gauge(prefix :+ "ledger_end_sequential_id", 0L)(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary =
      "The lag between the record time of a transaction and the wall-clock time registered at the ingestion phase to the index db (in milliseconds).",
    description = """Depending on the systemic clock skew between different machines, this value
                    |can be negative.""",
    qualification = Debug,
  )
  val currentRecordTimeLag: Gauge[Long] = NoOpGauge(prefix :+ "current_record_time_lag", 0)

  factory.gaugeWithSupplier(
    prefix :+ "current_record_time_lag",
    () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.getValue,
  )(MetricsContext.Empty)
}
