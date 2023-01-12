// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Traffic}
import com.daml.metrics.api.MetricHandle.{Counter, Factory}
import com.daml.metrics.api.{MetricDoc, MetricName}

class LAPIMetrics(val prefix: MetricName, val factory: Factory) {

  object threadpool {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "threadpool"

    val apiServices: MetricName = prefix :+ "api-services"

    val inMemoryFanOut: MetricName = prefix :+ "in_memory_fan_out"

    object indexBypass {
      private val prefix: MetricName = threadpool.prefix :+ "index_bypass"
      val prepareUpdates: MetricName = prefix :+ "prepare_updates"
      val updateInMemoryState: MetricName = prefix :+ "update_in_memory_state"
    }
  }

  object streams {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "streams"

    @MetricDoc.Tag(
      summary = "The number of the transaction trees sent over the ledger api.",
      description = """The total number of the transaction trees sent over the ledger api streams
                      |to all clients.""",
      qualification = Traffic,
    )
    val transactionTrees: Counter = factory.counter(prefix :+ "transaction_trees_sent")

    @MetricDoc.Tag(
      summary = "The number of the flat transactions sent over the ledger api.",
      description = """The total number of the flat transaction sent over the ledger api streams to
                      |all clients.""",
      qualification = Traffic,
    )
    val transactions: Counter = factory.counter(prefix :+ "transactions_sent")

    @MetricDoc.Tag(
      summary = "The number of the command completions sent by the ledger api.",
      description = """The total number of completions sent over the ledger api streams to all
                      |clients.""",
      qualification = Traffic,
    )
    val completions: Counter = factory.counter(prefix :+ "completions_sent")

    @MetricDoc.Tag(
      summary = "The number of the active contracts sent by the ledger api.",
      description = """The total number of active contracts sent over the ledger api streams to all
                      |clients.""",
      qualification = Traffic,
    )
    val acs: Counter = factory.counter(prefix :+ "acs_sent")

    val activeName: MetricName = prefix :+ "active"

    @MetricDoc.Tag(
      summary = "The number of the actice streams served by the ledger api.",
      description = "The number of ledger api streams currently being served to all clients.",
      qualification = Debug,
    )
    val active: Counter = factory.counter(activeName)
  }
}
