// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.{Counter, Timer, VarGauge}

import com.codahale.metrics.MetricRegistry

class IndexMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {

  @MetricDoc.Tag(
    summary = "The buffer size for transaction trees requests.",
    description = """An Akka stream buffer is added at the end of all streaming queries, allowing
                    |to absorb temporary downstream backpressure (e.g. when the client is
                    |slower than upstream delivery throughput). This metric gauges the
                    |size of the buffer for queries requesting transaction trees.""",
    qualification = Debug,
  )
  val transactionTreesBufferSize: Counter =
    counter(prefix :+ "transaction_trees_buffer_size")

  @MetricDoc.Tag(
    summary = "The buffer size for flat transactions requests.",
    description = """An Akka stream buffer is added at the end of all streaming queries, allowing
                    |to absorb temporary downstream backpressure (e.g. when the client is
                    |slower than upstream delivery throughput). This metric gauges the
                    |size of the buffer for queries requesting flat transactions in a specific
                    |period of time that satisfy a given predicate.""",
    qualification = Debug,
  )
  val flatTransactionsBufferSize: Counter =
    counter(prefix :+ "flat_transactions_buffer_size")

  @MetricDoc.Tag(
    summary = "The buffer size for active contracts requests.",
    description = """An Akka stream buffer is added at the end of all streaming queries, allowing
                    |to absorb temporary downstream backpressure (e.g. when the client is
                    |slower than upstream delivery throughput). This metric gauges the
                    |size of the buffer for queries requesting active contracts that transactions
                    |satisfying a given predicate.""",
    qualification = Debug,
  )
  val activeContractsBufferSize: Counter =
    counter(prefix :+ "active_contracts_buffer_size")

  @MetricDoc.Tag(
    summary = "The buffer size for completions requests.",
    description = """An Akka stream buffer is added at the end of all streaming queries, allowing
                    |to absorb temporary downstream backpressure (e.g. when the client is
                    |slower than upstream delivery throughput). This metric gauges the
                    |size of the buffer for queries requesting the completed commands in a specific
                    |period of time.""",
    qualification = Debug,
  )
  val completionsBufferSize: Counter =
    counter(prefix :+ "completions_buffer_size")

  object db extends IndexDBMetrics(prefix :+ "db", registry)

  @MetricDoc.Tag(
    summary = "The sequential id of the current ledger end kept in memory.",
    description = """The ledger end's sequential id is a monotonically increasing integer value
                    |representing the sequential id ascribed to the most recent ledger event
                    |ingested by the index db. Please note, that only a subset of all ledger events
                    |are ingested and given a sequential id. These are: creates, consuming
                    |exercises, non-consuming exercises and divulgence events. This value can
                    |be treated as a counter of all such events visible to a given participant.
                    |This metric exposes the latest ledger end's sequential id registered in the
                    |in-memory data set.""",
    qualification = Debug,
  )
  val ledgerEndSequentialId: VarGauge[Long] =
    varGauge(prefix :+ "ledger_end_sequential_id", 0)

  object lfValue {
    private val prefix = IndexMetrics.this.prefix :+ "lf_value"

    @MetricDoc.Tag(
      summary = "The time to compute an interface view while serving transaction streams.",
      description = """Transaction API allows clients to request events by interface-id. When an
                      |event matches the interface - an interface view is computed, which adds to
                      |the latency. This metric represents the time for each such computation.""",
      qualification = Debug,
    )
    val computeInterfaceView: Timer = timer(prefix :+ "compute_interface_view")
  }

  object packageMetadata {
    private val prefix = IndexMetrics.this.prefix :+ "package_metadata"

    @MetricDoc.Tag(
      summary = "The time to decode a package archive to extract metadata information.",
      description = """This metric represents the time spent scanning each uploaded package for new
                      |interfaces and corresponding templates.""",
      qualification = Debug,
    )
    val decodeArchive: Timer = timer(prefix :+ "decode_archive")

    @MetricDoc.Tag(
      summary = "The time to initialize package metadata view.",
      description = """As the mapping between interfaces and templates is not persistent - it is
                      |computed for each Indexer restart by loading all packages which were ever
                      |uploaded and scanning them to extract metadata information.""",
      qualification = Debug,
    )
    val viewInitialisation: Timer = timer(prefix :+ "view_init")
  }
}
