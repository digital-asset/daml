// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

class ParallelIndexerHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val inputMappingPrefix: MetricName = prefix :+ "inputmapping"

  private[metrics] val inputMappingBatchSize: Item = Item(
    inputMappingPrefix :+ "batch_size",
    summary = "The batch sizes in the indexer.",
    description = """The number of state updates contained in a batch used in the indexer for
                      |database submission.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val seqMappingDuration: Item = Item(
    prefix :+ "seqmapping" :+ "duration",
    summary = "The duration of the seq-mapping stage.",
    description = """The time that a batch of updates spends in the seq-mapping stage of the
                      |indexer.""",
    qualification = MetricQualification.Debug,
  )

}

class ParallelIndexerMetrics(
    histograms: ParallelIndexerHistograms,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {
  import MetricsContext.Implicits.empty
  private val prefix = histograms.prefix

  val initialization = new DatabaseMetrics(prefix :+ "initialization", openTelemetryMetricsFactory)

  // Number of state updates persisted to the database
  // (after the effect of the corresponding Update is persisted into the database,
  // and before this effect is visible via moving the ledger end forward)
  val updates: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "updates",
      summary = "The number of the state updates persisted to the database.",
      description = """The number of the state updates persisted to the database. There are
                    |updates such as accepted transactions, configuration changes,
                    |party allocations, rejections, etc, but they also include synthetic events
                    |when the node learned about the sequencer clock advancing without any actual
                    |ledger event such as due to submission receipts or time proofs.""",
      qualification = MetricQualification.Traffic,
    )
  )

  val inputBufferLength: Counter =
    openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "input_buffer_length",
        summary = "The number of elements in the queue in front of the indexer.",
        description = """The indexer has a queue in order to absorb the back pressure and facilitate
                      |batch formation during the database ingestion.""",
        qualification = MetricQualification.Saturation,
      )
    )

  val outputBatchedBufferLength: Counter =
    openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "output_batched_buffer_length",
        summary =
          "The size of the queue between the indexer and the in-memory state updating flow.",
        description =
          """This counter counts batches of updates passed to the in-memory flow. Batches
                      |are dynamically-sized based on amount of backpressure exerted by the
                      |downstream stages of the flow.""",
        qualification = MetricQualification.Debug,
      )
    )

  // Input mapping stage
  // Translating state updates to data objects corresponding to individual SQL insert statements
  object inputMapping {

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = histograms.inputMappingPrefix :+ "executor"

    val batchSize: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.inputMappingBatchSize.info)
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

    val duration: Timer = openTelemetryMetricsFactory.timer(histograms.seqMappingDuration.info)
  }

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix :+ "ingestion", openTelemetryMetricsFactory)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix :+ "tail_ingestion", openTelemetryMetricsFactory)

  // Post Processing end ingestion stage
  // The throttled update of post processing end parameter
  val postProcessingEndIngestion =
    new DatabaseMetrics(prefix :+ "post_processing_end_ingestion", openTelemetryMetricsFactory)

  val indexerQueueBlocked = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_blocked",
      summary = "The amount of blocked enqueue operations for the indexer queue.",
      description =
        """Indexer queue exerts backpressure by blocking asynchronous enqueue operations.
        |This meter measures the amount of such blocked operations, signalling backpressure
        |materializing from downstream.""",
      qualification = MetricQualification.Debug,
    )
  )

  val indexerQueueBuffered = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_buffered",
      summary = "The size of the buffer before the indexer.",
      description =
        """This buffer is located before the indexer, increasing amount signals backpressure mounting.""",
      qualification = MetricQualification.Debug,
    )
  )

  val indexerQueueUncommitted = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_uncommitted",
      summary = "The amount of entries which are uncommitted for the indexer.",
      description =
        """Uncommitted entries contain all blocked, buffered and submitted, but not yet committed entries.
        |This amount signals the momentum of stream processing, and has a theoretical maximum defined by all
        |the queue perameters.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )
}
