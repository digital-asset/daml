// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.{Counter, Histogram, Timer}

import com.codahale.metrics.MetricRegistry

@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.wait"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.exec"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.translation"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.compression"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.commit"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.query"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.executor.submitted"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.executor.running"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.executor.completed"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.executor.idle"
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.executor.duration"
)
class ParallelIndexerMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.DropwizardFactory {
  val initialization = new DatabaseMetrics(prefix, "initialization", registry)

  // Number of state updates persisted to the database
  // (after the effect of the corresponding Update is persisted into the database,
  // and before this effect is visible via moving the ledger end forward)
  @MetricDoc.Tag(
    summary = "The number of the state updates persisted to the database.",
    description = """The number of the state updates persisted to the database. There are
                    |updates such as accepted transactions, configuration changes, package uloads,
                    |party allocations, rejections, etc.""",
    qualification = Debug,
  )
  val updates: Counter = counter(prefix :+ "updates")

  @MetricDoc.Tag(
    summary = "The number of elements in the queue in front of the indexer.",
    description = """The indexer has a queue in order to absorb the back pressure and facilitate
                    |batch formation during the database ingestion.""",
    qualification = Debug,
  )
  val inputBufferLength: Counter = counter(prefix :+ "input_buffer_length")

  @MetricDoc.Tag(
    summary = "The size of the queue between the indexer and the in-memory state updating flow.",
    description = """This counter counts batches of updates passed to the in-memory flow. Batches
                    |are dynamically-sized based on amount of backpressure exerted by the
                    |downstream stages of the flow.""",
    qualification = Debug,
  )
  val outputBatchedBufferLength: Counter = counter(prefix :+ "output_batched_buffer_length")

  // Input mapping stage
  // Translating state updates to data objects corresponding to individual SQL insert statements
  object inputMapping {
    private val prefix: MetricName = ParallelIndexerMetrics.this.prefix :+ "inputmapping"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"
    val instrumentedExecutorServiceForDocs = new InstrumentedExecutorServiceForDocs(executor)

    @MetricDoc.Tag(
      summary = "The batch sizes in the indexer.",
      description = """The number of state updates contained in a batch used in the indexer for
                      |database submission.""",
      qualification = Debug,
    )
    val batchSize: Histogram = histogram(prefix :+ "batch_size")
  }

  // Batching stage
  // Translating batch data objects to db-specific DTO batches
  object batching {
    private val prefix: MetricName = ParallelIndexerMetrics.this.prefix :+ "batching"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"
    val instrumentedExecutorServiceForDocs = new InstrumentedExecutorServiceForDocs(executor)
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
    val duration: Timer = timer(prefix :+ "duration")
  }

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix, "ingestion", registry)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix, "tail_ingestion", registry)
}
