// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricHandle.{Counter, Gauge, Timer}

class IndexMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.DropwizardFactory {
  val transactionTreesBufferSize: Counter =
    counter(prefix :+ "transaction_trees_buffer_size")
  val flatTransactionsBufferSize: Counter =
    counter(prefix :+ "flat_transactions_buffer_size")
  val activeContractsBufferSize: Counter =
    counter(prefix :+ "active_contracts_buffer_size")
  val completionsBufferSize: Counter =
    counter(prefix :+ "completions_buffer_size")

  object db extends IndexDBMetrics(prefix :+ "db", registry)

  val ledgerEndSequentialId: Gauge[Long] =
    gauge(prefix :+ "ledger_end_sequential_id", 0)

  object lfValue {
    private val prefix = IndexMetrics.this.prefix :+ "lf_value"

    val computeInterfaceView: Timer = timer(prefix :+ "compute_interface_view")
  }

  object packageMetadata {
    private val prefix = IndexMetrics.this.prefix :+ "package_metadata"

    val decodeArchive: Timer = timer(prefix :+ "decode_archive")
    val viewInitialisation: Timer = timer(prefix :+ "view_init")
  }
}
