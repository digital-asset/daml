// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Gauge}

import com.codahale.metrics.MetricRegistry

import java.time.Instant

class IndexerMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  val lastReceivedRecordTime: Gauge[VarGauge[Long], Long] =
    varGauge(prefix :+ "last_received_record_time", 0)

  val lastReceivedOffset: Gauge[VarGauge[String], String] =
    varGauge(prefix :+ "last_received_offset", "<none>")

  val ledgerEndSequentialId: Gauge[VarGauge[Long], Long] =
    varGauge(prefix :+ "ledger_end_sequential_id", 0)

  gauge(
    prefix :+ "current_record_time_lag",
    () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.metric.getValue,
  )
}
