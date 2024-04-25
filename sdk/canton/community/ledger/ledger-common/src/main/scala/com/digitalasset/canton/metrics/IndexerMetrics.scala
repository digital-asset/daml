// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class IndexerMetrics(
    prefix: MetricName,
    factory: LabeledMetricsFactory,
) {

  import MetricsContext.Implicits.empty

  val lastReceivedRecordTime: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "last_received_record_time",
        summary =
          "The time of the last event ingested by the index db (in milliseconds since EPOCH).",
        description = """The last received record time is a monotonically increasing integer
                      |value that represents the record time of the last event ingested by the index
                      |db. It is measured in milliseconds since the EPOCH time.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val ledgerEndSequentialId: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "ledger_end_sequential_id",
        summary = "The sequential id of the current ledger end kept in the database.",
        description = """The ledger end's sequential id is a monotonically increasing integer value
                      |representing the sequential id ascribed to the most recent ledger event
                      |ingested by the index db. Please note, that only a subset of all ledger events
                      |are ingested and given a sequential id. These are: creates, consuming
                      |exercises, non-consuming exercises and divulgence events. This value can be
                      |treated as a counter of all such events visible to a given participant. This
                      |metric exposes the latest ledger end's sequential id registered in the
                      |database.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

}
