// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

class IndexerHistograms(val prefix: MetricName)(implicit
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

class IndexerMetrics(
    histograms: IndexerHistograms,
    factory: LabeledMetricsFactory,
) extends HasDocumentedMetrics {

  import MetricsContext.Implicits.empty

  private val prefix = histograms.prefix

  val initialization = new DatabaseMetrics(prefix :+ "initialization", factory)

  // Number of state updates persisted to the database
  // (after the effect of the corresponding Update is persisted into the database,
  // and before this effect is visible via moving the ledger end forward)
  val updates: Counter = factory.counter(
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

  val outputBatchedBufferLength: Counter =
    factory.counter(
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
      factory.histogram(histograms.inputMappingBatchSize.info)
  }

  // Batching stage
  // Translating batch data objects to db-specific DTO batches
  object batching {
    private val prefix: MetricName = IndexerMetrics.this.prefix :+ "batching"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"
  }

  // Sequence Mapping stage
  object seqMapping {

    val duration: Timer = factory.timer(histograms.seqMappingDuration.info)
  }

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix :+ "ingestion", factory)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix :+ "tail_ingestion", factory)

  // Post Processing end ingestion stage
  // The throttled update of post processing end parameter
  val postProcessingEndIngestion =
    new DatabaseMetrics(prefix :+ "post_processing_end_ingestion", factory)

  val indexerQueueBlocked: MetricHandle.Meter = factory.meter(
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

  val indexerQueueBuffered: MetricHandle.Meter = factory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_buffered",
      summary = "The size of the buffer before the indexer.",
      description =
        """This buffer is located before the indexer, increasing amount signals backpressure mounting.""",
      qualification = MetricQualification.Debug,
    )
  )

  val indexerQueueUncommitted: MetricHandle.Meter = factory.meter(
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

  val ledgerEndSequentialId: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "ledger_end_sequential_id",
        summary = "The sequential id of the current ledger end kept in the database.",
        description = """The ledger end's sequential id is a monotonically increasing integer value
                      |representing the sequential id ascribed to the most recent ledger event
                      |ingested by the index db. Please note, that only a subset of all ledger events
                      |are ingested and given a sequential id. These are: creates, consuming
                      |exercises, non-consuming exercises and divulgence events. This value can be
                      |treated as a counter of all such events visible to a given participant. This
                      |metric exposes the latest ledger end's sequential id registered in the
                      |database.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val meteredEventsMeter: MetricHandle.Meter = factory.meter(
    MetricInfo(
      prefix :+ "metered_events",
      summary = "Number of ledger events that are metered.",
      description = """Represents the number of events that will be included in the metering report.
          |This is an estimate of the total number and not a substitute for the metering report.""",
      qualification = MetricQualification.Debug,
      labelsWithDescription = Map(
        "participant_id" -> "The id of the participant.",
        "application_id" -> "The application generating the events.",
      ),
    )
  )

  val eventsMeter: MetricHandle.Meter =
    factory.meter(
      MetricInfo(
        prefix :+ "events",
        summary = "Number of ledger events processed.",
        description =
          "Represents the total number of ledger events processed (transactions, reassignments, party allocations).",
        qualification = MetricQualification.Debug,
        labelsWithDescription = Map(
          "participant_id" -> "The id of the participant.",
          "application_id" -> "The application generating the events.",
          "event_type" -> "The type of ledger event processed (transaction, reassignment, party_allocation).",
          "status" -> "Indicates if the event was accepted or not. Possible values accepted|rejected.",
        ),
      )
    )
}

object IndexerMetrics {

  object Labels {
    val applicationId = "application_id"
    val grpcCode = "grpc_code"
    object eventType {

      val key = "event_type"

      val partyAllocation = "party_allocation"
      val transaction = "transaction"
      val reassignment = "reassignment"
      val topologyTransaction = "topology_transaction"
    }

    object status {
      val key = "status"

      val accepted = "accepted"
      val rejected = "rejected"
    }

  }
}
