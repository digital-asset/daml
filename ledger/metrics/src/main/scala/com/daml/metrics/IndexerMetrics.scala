// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Gauge}

import com.codahale.metrics.MetricRegistry

import java.time.Instant

class IndexerMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  // TODO: use varGauge
  val lastReceivedRecordTime = new VarGauge[Long](0)
  registry.register(prefix :+ "last_received_record_time", lastReceivedRecordTime)

  // TODO: use varGauge
  val lastReceivedOffset = new VarGauge[String]("<none>")
  registry.register(prefix :+ "last_received_offset", lastReceivedOffset)

  // TODO: register Gauge to return GaugeM and add into MetricHandle.Factory as varGauge
  registerGauge(
    prefix :+ "current_record_time_lag",
    () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.getValue,
    registry,
  )

  val ledgerEndSequentialId = new VarGauge[Long](0L)
  registry.register(prefix :+ "ledger_end_sequential_id", ledgerEndSequentialId)
}
