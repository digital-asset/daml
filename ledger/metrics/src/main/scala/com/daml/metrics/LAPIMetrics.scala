// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Counter, Timer}

import com.codahale.metrics.MetricRegistry

class LAPIMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.DropwizardFactory {
  def forMethod(name: String): Timer = timer(prefix :+ name)

  object return_status {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "return_status"
    def forCode(code: String): Counter = counter(prefix :+ code)
  }

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

    val transactionTrees: Counter = counter(prefix :+ "transaction_trees_sent")
    val transactions: Counter = counter(prefix :+ "transactions_sent")
    val completions: Counter = counter(prefix :+ "completions_sent")
    val acs: Counter = counter(prefix :+ "acs_sent")

    val activeName: MetricName = prefix :+ "active"
    val active: Counter = counter(activeName)
  }
}
