// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.metrics.HistogramInventory.Item

class IndexHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private val lfValuePrefix = prefix :+ "lf_value"
  private[metrics] val db = new IndexDBHistograms(prefix :+ "db")

  private[metrics] val computeInterfaceView: Item = Item(
    lfValuePrefix :+ "compute_interface_view",
    summary = "The time to compute an interface view while serving transaction streams.",
    description = """Transaction API allows clients to request events by interface-id. When an
                          |event matches the interface - an interface view is computed, which adds to
                          |the latency. This metric represents the time for each such computation.""",
    qualification = MetricQualification.Debug,
  )
}

class IndexMetrics(
    inventory: IndexHistograms,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  import MetricsContext.Implicits.empty
  private val prefix = inventory.prefix

  val transactionTreesBufferSize: Counter =
    openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "transaction_trees_buffer_size",
        summary = "The buffer size for transaction trees requests.",
        description =
          """An Pekko stream buffer is added at the end of all streaming queries, allowing
                      |to absorb temporary downstream backpressure (e.g. when the client is
                      |slower than upstream delivery throughput). This metric gauges the
                      |size of the buffer for queries requesting transaction trees.""",
        qualification = MetricQualification.Debug,
      )
    )

  val flatTransactionsBufferSize: Counter =
    openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "flat_transactions_buffer_size",
        summary = "The buffer size for flat transactions requests.",
        description =
          """An Pekko stream buffer is added at the end of all streaming queries, allowing
                      |to absorb temporary downstream backpressure (e.g. when the client is
                      |slower than upstream delivery throughput). This metric gauges the
                      |size of the buffer for queries requesting flat transactions in a specific
                      |period of time that satisfy a given predicate.""",
        qualification = MetricQualification.Debug,
      )
    )

  val activeContractsBufferSize: Counter =
    openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "active_contracts_buffer_size",
        summary = "The buffer size for active contracts requests.",
        description =
          """An Pekko stream buffer is added at the end of all streaming queries, allowing
                      |to absorb temporary downstream backpressure (e.g. when the client is
                      |slower than upstream delivery throughput). This metric gauges the
                      |size of the buffer for queries requesting active contracts that transactions
                      |satisfying a given predicate.""",
        qualification = MetricQualification.Debug,
      )
    )

  val completionsBufferSize: Counter =
    openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "completions_buffer_size",
        summary = "The buffer size for completions requests.",
        description =
          """An Pekko stream buffer is added at the end of all streaming queries, allowing
                      |to absorb temporary downstream backpressure (e.g. when the client is
                      |slower than upstream delivery throughput). This metric gauges the
                      |size of the buffer for queries requesting the completed commands in a specific
                      |period of time.""",
        qualification = MetricQualification.Debug,
      )
    )

  object db
      extends IndexDBMetrics(
        inventory.db,
        openTelemetryMetricsFactory,
      )

  val ledgerEndSequentialId: Gauge[Long] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "ledger_end_sequential_id",
        summary = "The sequential id of the current ledger end kept in memory.",
        description = """The ledger end's sequential id is a monotonically increasing integer value
                      |representing the sequential id ascribed to the most recent ledger event
                      |ingested by the index db. Please note, that only a subset of all ledger events
                      |are ingested and given a sequential id. These are: creates, consuming
                      |exercises, non-consuming exercises and divulgence events. This value can
                      |be treated as a counter of all such events visible to a given participant.
                      |This metric exposes the latest ledger end's sequential id registered in the
                      |in-memory data set.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  object lfValue {

    val computeInterfaceView: Timer =
      openTelemetryMetricsFactory.timer(inventory.computeInterfaceView.info)
  }
}
