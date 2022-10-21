// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.{Counter, DropwizardCounter, DropwizardTimer, Timer}

class LAPIMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.DropwizardFactory {

  @MetricDoc.Tag(
    summary = "The time spent serving a ledger api grpc request.",
    description = """The time spent servicing a particular type of ledger api grpc request. Unary
                    |methods report the time to serve the request, streaming methods measure the
                    |time to return the first response.""",
    qualification = Debug,
  )
  val forMethodForDocs: Timer = DropwizardTimer(prefix :+ "<service_method>", null)
  def forMethod(name: String): Timer = timer(prefix :+ name)

  object return_status {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "return_status"

    @MetricDoc.Tag(
      summary = "The number of ledger api grpc responses with this code.",
      description = """This group of metrics counts the total number gRPC status codes returned by
                      |the ledger api.""",
      qualification = Debug,
    )
    val forCodeForDocs = DropwizardCounter(prefix :+ "<gRPC_status_code>", null)

    def forCode(code: String): Counter = counter(prefix :+ code)
  }

  object threadpool {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "threadpool"

    val instrumentedExecutorServiceForDocs = new InstrumentedExecutorServiceForDocs(
      prefix :+ "<threadpool>"
    )

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
      qualification = Debug,
    )
    val transactionTrees: Counter = counter(prefix :+ "transaction_trees_sent")

    @MetricDoc.Tag(
      summary = "The number of the flat transactions sent over the ledger api.",
      description = """The total number of the flat transaction sent over the ledger api streams to
                      |all clients.""",
      qualification = Debug,
    )
    val transactions: Counter = counter(prefix :+ "transactions_sent")

    @MetricDoc.Tag(
      summary = "The number of the command completions sent by the ledger api.",
      description = """The total number of completions sent over the ledger api streams to all
                      |clients.""",
      qualification = Debug,
    )
    val completions: Counter = counter(prefix :+ "completions_sent")

    @MetricDoc.Tag(
      summary = "The number of the actice contracts sent by the ledger api.",
      description = """The total number of active contracts sent over the ledger api streams to all
                      |clients.""",
      qualification = Debug,
    )
    val acs: Counter = counter(prefix :+ "acs_sent")

    val activeName: MetricName = prefix :+ "active"

    @MetricDoc.Tag(
      summary = "The number of the actice streams served by the ledger api.",
      description = "The number of ledger api streams currently being served to all clients.",
      qualification = Debug,
    )
    val active: Counter = counter(activeName)
  }
}
