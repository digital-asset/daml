// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory}
import com.daml.metrics.api.{
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

class LAPIMetrics private[metrics] (
    val prefix: MetricName,
    val metricsFactory: LabeledMetricsFactory,
) {

  import MetricsContext.Implicits.empty

  // Private constructor to avoid being instantiated multiple times by accident
  final class ThreadpoolMetrics private[LAPIMetrics] {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "threadpool"

    val apiQueryServices: MetricName = prefix :+ "api_query_services"

    // Private constructor to avoid being instantiated multiple times by accident
    final class IndexBypassMetrics private[ThreadpoolMetrics] {
      private val prefix: MetricName = ThreadpoolMetrics.this.prefix :+ "index_bypass"
      val prepareUpdates: MetricName = prefix :+ "prepare_updates"
      val updateInMemoryState: MetricName = prefix :+ "update_in_memory_state"
    }
    val indexBypass = new IndexBypassMetrics
  }

  val threadpool: ThreadpoolMetrics = new ThreadpoolMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class StreamsMetrics private[LAPIMetrics] {
    private val prefix: MetricName = LAPIMetrics.this.prefix :+ "streams"

    val transactionTrees: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "transaction_trees_sent",
        summary = "The number of the transaction trees sent over the ledger api.",
        description = """The total number of the transaction trees sent over the ledger api streams
                      |to all clients.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val updates: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "updates_sent",
        summary = "The number of the flat updates sent over the ledger api.",
        description = """The total number of the flat updates sent over the ledger api streams to
                      |all clients.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val updateTrees: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "update_trees_sent",
        summary = "The number of the update trees sent over the ledger api.",
        description = """The total number of the update trees sent over the ledger api streams to
                        |all clients.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val completions: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "completions_sent",
        summary = "The number of the command completions sent by the ledger api.",
        description = """The total number of completions sent over the ledger api streams to all
                      |clients.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val acs: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "acs_sent",
        summary = "The number of the active contracts sent by the ledger api.",
        description =
          """The total number of active contracts sent over the ledger api streams to all
                      |clients.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val activeName: MetricName = prefix :+ "active"

    val active: MetricHandle.Gauge[Int] =
      metricsFactory.gauge(
        MetricInfo(
          activeName,
          summary = "The number of the active streams served by the ledger api.",
          description = "The number of ledger api streams currently being served to all clients.",
          qualification = MetricQualification.Debug,
        ),
        0,
      )
  }

  val streams: StreamsMetrics = new StreamsMetrics
}
