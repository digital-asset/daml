// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.VarGauge

import com.codahale.metrics.MetricRegistry

import java.time.Instant

class IndexerMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  val lastReceivedRecordTime: VarGauge[Long] =
    varGauge(prefix :+ "last_received_record_time", 0)

  val lastReceivedOffset: VarGauge[String] =
    varGauge(prefix :+ "last_received_offset", "<none>")

  val ledgerEndSequentialId: VarGauge[Long] =
    varGauge(prefix :+ "ledger_end_sequential_id", 0)

  gaugeWithSupplier(
    prefix :+ "current_record_time_lag",
    () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.metric.getValue,
  )
}
