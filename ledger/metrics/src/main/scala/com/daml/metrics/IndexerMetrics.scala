// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricHandle.Gauge

class IndexerMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  val lastReceivedRecordTime: Gauge[Long] =
    gauge(prefix :+ "last_received_record_time", 0)

  val lastReceivedOffset: Gauge[String] =
    gauge(prefix :+ "last_received_offset", "<none>")

  val ledgerEndSequentialId: Gauge[Long] =
    gauge(prefix :+ "ledger_end_sequential_id", 0)

  gaugeWithSupplier(
    prefix :+ "current_record_time_lag",
    () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.getValue,
  )
}
