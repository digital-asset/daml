// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Latency, Saturation, Traffic}
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Timer}
import com.daml.metrics.api.dropwizard.{DropwizardFactory, DropwizardTimer}
import com.daml.metrics.api.{MetricDoc, MetricName}

@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>.executor",
  groupableClass = classOf[InstrumentedExecutorServiceForDocs],
)
@MetricDoc.GroupTag(
  representative = "daml.parallel_indexer.<stage>",
  groupableClass = classOf[DatabaseMetrics],
)
class ParallelIndexerMetrics(val prefix: MetricName, override val registry: MetricRegistry)
    extends DropwizardFactory {
  val initialization = new DatabaseMetrics(prefix, "initialization", registry)

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
  val updates: Counter = counter(prefix :+ "updates")

  @MetricDoc.Tag(
    summary = "The number of elements in the queue in front of the indexer.",
    description = """The indexer has a queue in order to absorb the back pressure and facilitate
                    |batch formation during the database ingestion.""",
    qualification = Saturation,
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

  @MetricDoc.Tag(
    summary = "The time needed to run the SQL query and read the result.",
    description = """This metric encompasses the time measured by `query` and `commit` metrics.
                    |Additionally it includes the time needed to obtain the DB connection,
                    |optionally roll it back and close the connection at the end.""",
    qualification = Latency,
  )
  val ingestionExecForDocs: Timer = DropwizardTimer(prefix :+ "ingestion" :+ "exec", null)

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix, "ingestion", registry)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix, "tail_ingestion", registry)
}
