// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Counter, Histogram, Timer}

import com.codahale.metrics.MetricRegistry

class ParallelIndexerMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.DropwizardFactory {
  val initialization = new DatabaseMetrics(prefix, "initialization", registry)

  // Number of state updates persisted to the database
  // (after the effect of the corresponding Update is persisted into the database,
  // and before this effect is visible via moving the ledger end forward)
  val updates: Counter = counter(prefix :+ "updates")

  // The size of the queue before the indexer
  val inputBufferLength: Counter = counter(prefix :+ "input_buffer_length")

  /** The size of the queue after the indexer and before the in-memory state updating flow.
    * As opposed to [[inputBufferLength]], this counter counts batches, which are dynamically-sized
    * (for batch size, see [[inputMapping.batchSize]]).
    */
  val outputBatchedBufferLength: Counter = counter(prefix :+ "output_batched_buffer_length")

  // Input mapping stage
  // Translating state updates to data objects corresponding to individual SQL insert statements
  object inputMapping {
    private val prefix: MetricName = ParallelIndexerMetrics.this.prefix :+ "inputmapping"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"

    // The batch size, i.e., the number of state updates per database submission
    val batchSize: Histogram = histogram(prefix :+ "batch_size")
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

    // The latency, which during an update element is residing in the seq-mapping-stage.
    val duration: Timer = timer(prefix :+ "duration")
  }

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix, "ingestion", registry)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix, "tail_ingestion", registry)
}
