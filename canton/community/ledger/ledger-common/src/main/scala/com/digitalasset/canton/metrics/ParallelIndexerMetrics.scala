// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Saturation, Traffic}
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.{MetricDoc, MetricName}

import scala.annotation.nowarn

class ParallelIndexerMetrics(
    prefix: MetricName,
    @nowarn("cat=deprecation") metricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  val initialization = new DatabaseMetrics(prefix :+ "initialization", labeledMetricsFactory)

  // Number of state updates persisted to the database
  // (after the effect of the corresponding Update is persisted into the database,
  // and before this effect is visible via moving the ledger end forward)
  @MetricDoc.Tag(
    summary = "The number of the state updates persisted to the database.",
    description = """The number of the state updates persisted to the database. There are
                    |updates such as accepted transactions, configuration changes, package uloads,
                    |party allocations, rejections, etc.""",
    qualification = Traffic,
  )
  val updates: Counter = metricsFactory.counter(prefix :+ "updates")

  @MetricDoc.Tag(
    summary = "The number of elements in the queue in front of the indexer.",
    description = """The indexer has a queue in order to absorb the back pressure and facilitate
                    |batch formation during the database ingestion.""",
    qualification = Saturation,
  )
  val inputBufferLength: Counter = metricsFactory.counter(prefix :+ "input_buffer_length")

  @MetricDoc.Tag(
    summary = "The size of the queue between the indexer and the in-memory state updating flow.",
    description = """This counter counts batches of updates passed to the in-memory flow. Batches
                    |are dynamically-sized based on amount of backpressure exerted by the
                    |downstream stages of the flow.""",
    qualification = Debug,
  )
  val outputBatchedBufferLength: Counter =
    metricsFactory.counter(prefix :+ "output_batched_buffer_length")

  // Input mapping stage
  // Translating state updates to data objects corresponding to individual SQL insert statements
  object inputMapping {
    private val prefix: MetricName = ParallelIndexerMetrics.this.prefix :+ "inputmapping"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"

    @MetricDoc.Tag(
      summary = "The batch sizes in the indexer.",
      description = """The number of state updates contained in a batch used in the indexer for
                      |database submission.""",
      qualification = Debug,
    )
    val batchSize: Histogram = metricsFactory.histogram(prefix :+ "batch_size")
  }

  // Batching stage
  // Translating batch data objects to db-specific DTO batches
  object batching {
    private val prefix: MetricName = ParallelIndexerMetrics.this.prefix :+ "batching"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"
  }

  // Sequence Mapping stage
  object seqMapping {
    private val prefix: MetricName = ParallelIndexerMetrics.this.prefix :+ "seqmapping"

    @MetricDoc.Tag(
      summary = "The duration of the seq-mapping stage.",
      description = """The time that a batch of updates spends in the seq-mapping stage of the
                      |indexer.""",
      qualification = Debug,
    )
    val duration: Timer = metricsFactory.timer(prefix :+ "duration")
  }

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix :+ "ingestion", labeledMetricsFactory)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix :+ "tail_ingestion", labeledMetricsFactory)
}
