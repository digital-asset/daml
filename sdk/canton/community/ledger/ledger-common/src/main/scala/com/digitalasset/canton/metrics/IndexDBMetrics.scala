// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification, MetricsContext}

private[metrics] trait TransactionStreamsDbHistograms {

  protected implicit def inventory: HistogramInventory
  protected def prefix: MetricName

  private val flatTxStreamPrefix: MetricName = prefix :+ "flat_transactions_stream"

  private[metrics] val flatTxStreamTranslationTimer: Item = Item(
    flatTxStreamPrefix :+ "translation",
    summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
    description = """Some index database queries that target contracts and transactions involve a
            |Daml-LF translation step. For such queries this metric stands for the time it
            |takes to turn the serialized Daml-LF values into in-memory representation.""",
    qualification = MetricQualification.Debug,
  )

  private val treeTxStreamPrefix: MetricName = prefix :+ "tree_transactions_stream"

  private[metrics] val treeTxStreamTranslationTimer: Item = Item(
    treeTxStreamPrefix :+ "translation",
    summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
    description = """Some index database queries that target contracts and transactions involve a
            |Daml-LF translation step. For such queries this metric stands for the time it
            |takes to turn the serialized Daml-LF values into in-memory representation.""",
    qualification = MetricQualification.Debug,
  )

  private val reassignmentStreamPrefix: MetricName = prefix :+ "reassignment_stream"

  private[metrics] val reassignmentStreamTranslationTimer: Item = Item(
    reassignmentStreamPrefix :+ "translation",
    summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
    description = """Some index database queries that target contracts and transactions involve a
            |Daml-LF translation step. For such queries this metric stands for the time it
            |takes to turn the serialized Daml-LF values into in-memory representation.""",
    qualification = MetricQualification.Debug,
  )

}

final class IndexDBHistograms(prefix: MetricName)(implicit
    protected val inventory: HistogramInventory
) extends MainIndexDBHistograms(prefix)
    with TransactionStreamsDbHistograms

// Private constructor to avoid being instantiated multiple times by accident
final class IndexDBMetrics private[metrics] (
    override val inventory: IndexDBHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends MainIndexDBMetrics(inventory, openTelemetryMetricsFactory)
    with TransactionStreamsDbMetrics

trait TransactionStreamsDbMetrics {
  self: DatabaseMetricsFactory =>

  val inventory: TransactionStreamsDbHistograms
  val openTelemetryMetricsFactory: LabeledMetricsFactory

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  // Private constructor to avoid being instantiated multiple times by accident
  final class UpdatesAcsDeltaStreamMetrics private[TransactionStreamsDbMetrics] {
    val fetchEventCreateIdsStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_create_ids_stakeholder"
    )
    val fetchEventConsumingIdsStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_consuming_ids_stakeholder"
    )
    val fetchEventCreatePayloads: DatabaseMetrics = createDbMetrics("fetch_event_create_payloads")
    val fetchEventConsumingPayloads: DatabaseMetrics = createDbMetrics(
      "fetch_event_consuming_payloads"
    )

    val translationTimer: Timer =
      openTelemetryMetricsFactory.timer(inventory.flatTxStreamTranslationTimer.info)
  }

  val updatesAcsDeltaStream: UpdatesAcsDeltaStreamMetrics = new UpdatesAcsDeltaStreamMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class UpdatesLedgerEffectsStreamMetrics private[TransactionStreamsDbMetrics] {

    val fetchEventCreateIdsStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_create_ids_stakeholder"
    )
    val fetchEventCreateIdsNonStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_create_ids_non_stakeholder"
    )
    val fetchEventConsumingIdsStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_consuming_ids_stakeholder"
    )
    val fetchEventConsumingIdsNonStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_consuming_ids_non_stakeholder"
    )
    val fetchEventNonConsumingIds: DatabaseMetrics = createDbMetrics(
      "fetch_event_non_consuming_ids_informee"
    )
    val fetchEventCreatePayloads: DatabaseMetrics = createDbMetrics("fetch_event_create_payloads")
    val fetchEventConsumingPayloads: DatabaseMetrics = createDbMetrics(
      "fetch_event_consuming_payloads"
    )
    val fetchEventNonConsumingPayloads: DatabaseMetrics = createDbMetrics(
      "fetch_event_non_consuming_payloads"
    )

    val translationTimer: Timer =
      openTelemetryMetricsFactory.timer(inventory.treeTxStreamTranslationTimer.info)

  }

  val updatesLedgerEffectsStream: UpdatesLedgerEffectsStreamMetrics =
    new UpdatesLedgerEffectsStreamMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class ReassignmentStreamMetrics private[TransactionStreamsDbMetrics] {

    val fetchEventAssignIdsStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_assign_ids_stakeholder"
    )
    val fetchEventUnassignIdsStakeholder: DatabaseMetrics = createDbMetrics(
      "fetch_event_unassign_ids_stakeholder"
    )
    val fetchEventAssignPayloads: DatabaseMetrics = createDbMetrics("fetch_event_assign_payloads")
    val fetchEventUnassignPayloads: DatabaseMetrics = createDbMetrics(
      "fetch_event_unassign_payloads"
    )

    val translationTimer: Timer =
      openTelemetryMetricsFactory.timer(inventory.reassignmentStreamTranslationTimer.info)

  }

  val reassignmentStream: ReassignmentStreamMetrics = new ReassignmentStreamMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class TopologyTransactionsStreamMetrics private[TransactionStreamsDbMetrics] {
    val fetchTopologyPartyEventIds: DatabaseMetrics =
      createDbMetrics("fetch_topology_party_event_ids")

    val fetchTopologyPartyEventPayloads: DatabaseMetrics =
      createDbMetrics("fetch_topology_party_event_payloads")
  }

  val topologyTransactionsStream: TopologyTransactionsStreamMetrics =
    new TopologyTransactionsStreamMetrics
}

final class BatchLoaderMetricsInventory(parent: MetricName)(implicit
    inventory: HistogramInventory
) {
  private val prefix = parent :+ "batch"

  val bufferLength: Item =
    Item(
      prefix :+ "buffer_length",
      summary = "The number of the currently pending lookups.",
      description =
        "The number of the currently pending lookups in the batch-loading queue of the Contract Service.",
      qualification = MetricQualification.Debug,
    )

  val bufferCapacity: Item =
    Item(
      prefix :+ "buffer_capacity",
      summary = "The capacity of the lookup queue.",
      description = """The maximum number of elements that can be kept in the queue of lookups
                      |in the batch-loading queue of the Contract Service.""",
      qualification = MetricQualification.Debug,
    )

  val bufferDelay: Item =
    Item(
      prefix :+ "buffer_delay",
      summary = "The queuing delay for the lookup queue.",
      description =
        "The queuing delay for the pending lookups in the batch-loading queue of the Contract Service.",
      qualification = MetricQualification.Debug,
    )

  val batchSize: Item =
    Item(
      prefix :+ "batch_size",
      summary = "The batch sizes in the lookup batch-loading Contract Service.",
      description =
        """The number of lookups contained in a batch, used in the batch-loading Contract Service.""",
      qualification = MetricQualification.Debug,
    )
}

final class BatchLoaderMetrics(
    inventory: BatchLoaderMetricsInventory,
    factory: LabeledMetricsFactory,
) {
  val bufferLength: Counter = factory.counter(inventory.bufferLength.info)
  val bufferCapacity: Counter = factory.counter(inventory.bufferCapacity.info)
  val bufferDelay: Timer = factory.timer(inventory.bufferDelay.info)
  val batchSize: Histogram = factory.histogram(inventory.batchSize.info)
}

private[metrics] class MainIndexDBHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val lookupKey: Item = Item(
    prefix :+ "lookup_key",
    summary = "The time spent looking up a contract using its key.",
    description = """This metric exposes the time spent looking up a contract using its key in the
                      |index db. It is then used by the Daml interpreter when evaluating a command
                      |into a transaction.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val lookupActiveContract: Item = Item(
    prefix :+ "lookup_active_contract",
    summary = "The time spent fetching a contract using its id.",
    description = """This metric exposes the time spent fetching a contract using its id from the
            |index db. It is then used by the Daml interpreter when evaluating a command
            |into a transaction.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val activeContracts = new BatchLoaderMetricsInventory(
    prefix :+ "active_contract_lookup"
  )

  private[metrics] val activeContractKeys = new BatchLoaderMetricsInventory(
    prefix :+ "active_contract_keys_lookup"
  )

  private val translationPrefix = prefix :+ "translation"
  // TODO(#17635): It's not an IndexDB op anymore
  private[metrics] val getLfPackage: Item = Item(
    translationPrefix :+ "get_lf_package",
    summary = "The time needed to deserialize and decode a Daml-LF archive.",
    description = """A Daml archive before it can be used in the interpretation needs to be
                        |deserialized and decoded, in other words converted into the in-memory
                        |representation. This metric represents time necessary to do that.""",
    qualification = MetricQualification.Debug,
  )

  private val compressionPrefix: MetricName = prefix :+ "compression"

  private[metrics] val createArgumentCompressed: Item = Item(
    compressionPrefix :+ "create_argument_compressed",
    summary = "The size of the compressed arguments of a create event.",
    description = """Event information can be compressed by the indexer before storing it in the
              |database. This metric collects statistics about the size of compressed
              |arguments of a create event.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val createKeyValueCompressed: Item = Item(
    compressionPrefix :+ "create_key_value_compressed",
    summary = "The size of the compressed key value of a create event.",
    description = """Event information can be compressed by the indexer before storing it in the
              |database. This metric collects statistics about the size of compressed key
              |value of a create event.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val createKeyValueUncompressed: Item = Item(
    compressionPrefix :+ "create_key_value_uncompressed",
    summary = "The size of the decompressed key value of a create event.",
    description = """Event information can be compressed by the indexer before storing it in the
                        |database. This metric collects statistics about the size of decompressed key
                        |value of a create event.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val exerciseArgumentCompressed: Item = Item(
    compressionPrefix :+ "exercise_argument_compressed",
    summary = "The size of the compressed argument of an exercise event.",
    description = """Event information can be compressed by the indexer before storing it in the
              |database. This metric collects statistics about the size of compressed
              |arguments of an exercise event.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val exerciseArgumentUncompressed: Item = Item(
    compressionPrefix :+ "exercise_argument_uncompressed",
    summary = "The size of the decompressed argument of an exercise event.",
    description = """Event information can be compressed by the indexer before storing it in the
                        |database. This metric collects statistics about the size of decompressed
                        |arguments of an exercise event.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val exerciseResultCompressed: Item = Item(
    compressionPrefix :+ "exercise_result_compressed",
    summary = "The size of the compressed result of an exercise event.",
    description = """Event information can be compressed by the indexer before storing it in the
              |database. This metric collects statistics about the size of compressed
              |result of an exercise event.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val exerciseResultUncompressed: Item = Item(
    compressionPrefix :+ "exercise_result_uncompressed",
    summary = "The size of the decompressed result of an exercise event.",
    description = """Event information can be compressed by the indexer before storing it in the
              |database. This metric collects statistics about the size of compressed
              |result of an exercise event.""",
    qualification = MetricQualification.Debug,
  )

}

class MainIndexDBMetrics(
    inventory: MainIndexDBHistograms,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends DatabaseMetricsFactory(inventory.prefix, openTelemetryMetricsFactory) { self =>

  implicit val metricsContext: MetricsContext = MetricsContext.Empty
  private val prefix = inventory.prefix

  val lookupKey: Timer = openTelemetryMetricsFactory.timer(inventory.lookupKey.info)

  val lookupActiveContract: Timer =
    openTelemetryMetricsFactory.timer(inventory.lookupActiveContract.info)

  val activeContracts =
    new BatchLoaderMetrics(inventory.activeContracts, openTelemetryMetricsFactory)
  val activeContractKeys =
    new BatchLoaderMetrics(inventory.activeContractKeys, openTelemetryMetricsFactory)

  private val overall = createDbMetrics("all")
  val waitAll: Timer = overall.waitTimer
  val execAll: Timer = overall.executionTimer

  val getCompletions: DatabaseMetrics = createDbMetrics("get_completions")
  val getParticipantId: DatabaseMetrics = createDbMetrics("get_participant_id")
  val getLedgerEnd: DatabaseMetrics = createDbMetrics("get_ledger_end")
  val getCleanSynchronizerIndex: DatabaseMetrics = createDbMetrics("get_clean_synchronizer_index")
  val getTopologyEventPublishedOnRecordTime: DatabaseMetrics = createDbMetrics(
    "get_topology_event_published_on_record_time"
  )
  val getPostProcessingEnd: DatabaseMetrics = createDbMetrics("get_post_processing_end")
  val initializeLedgerParameters: DatabaseMetrics = createDbMetrics(
    "initialize_ledger_parameters"
  )
  val lookupConfiguration: DatabaseMetrics = createDbMetrics("lookup_configuration")

  val storePartyEntryDbMetrics: DatabaseMetrics = createDbMetrics(
    "store_party_entry"
  )
  val loadPartyEntries: DatabaseMetrics = createDbMetrics("load_party_entries")

  val storeTransactionDbMetrics: DatabaseMetrics = createDbMetrics("store_ledger_entry")

  val storeRejectionDbMetrics: DatabaseMetrics = createDbMetrics(
    "store_rejection"
  )
  val loadParties: DatabaseMetrics = createDbMetrics("load_parties")
  val loadAllParties: DatabaseMetrics = createDbMetrics("load_all_parties")
  val pruneDbMetrics: DatabaseMetrics = createDbMetrics("prune")
  val fetchPruningOffsetsMetrics: DatabaseMetrics = createDbMetrics("fetch_pruning_offsets")
  val lookupCreatedContractsDbMetrics: DatabaseMetrics = createDbMetrics("lookup_created_contracts")
  val lookupAssignedContractsDbMetrics: DatabaseMetrics = createDbMetrics(
    "lookup_assigned_contracts"
  )
  val lookupArchivedContractsDbMetrics: DatabaseMetrics = createDbMetrics(
    "lookup_archived_contracts"
  )
  val lookupContractByKeyDbMetrics: DatabaseMetrics = createDbMetrics(
    "lookup_contract_by_key"
  )

  val lookupPointwiseTransaction: DatabaseMetrics = createDbMetrics(
    "lookup_pointwise_transaction"
  )
  val lookupTransactionTreeById: DatabaseMetrics = createDbMetrics(
    "lookup_transaction_tree_by_id"
  )
  val getEventsByContractId: DatabaseMetrics = createDbMetrics("get_events_by_contract_id")
  val getActiveContracts: DatabaseMetrics = createDbMetrics("get_active_contracts")
  val getActiveContractIdsForCreated: DatabaseMetrics = createDbMetrics(
    "get_active_contract_ids_for_created"
  )
  val getActiveContractIdsForAssigned: DatabaseMetrics = createDbMetrics(
    "get_active_contract_ids_for_assigned"
  )
  val getActiveContractBatchForCreated: DatabaseMetrics = createDbMetrics(
    "get_active_contract_batch_for_created"
  )
  val getActiveContractBatchForAssigned: DatabaseMetrics = createDbMetrics(
    "get_active_contract_batch_for_assigned"
  )
  val getEventSeqIdRange: DatabaseMetrics = createDbMetrics("get_event_sequential_id_range")
  val getAcsEventSeqIdRange: DatabaseMetrics =
    createDbMetrics("get_acs_event_sequential_id_range")
  val loadStringInterningEntries: DatabaseMetrics = createDbMetrics(
    "load_string_interning_entries"
  )

  val meteringAggregator: DatabaseMetrics = createDbMetrics("metering_aggregator")
  val initializeMeteringAggregator: DatabaseMetrics = createDbMetrics(
    "initialize_metering_aggregator"
  )

  val getAssingIdsForOffsets: DatabaseMetrics = createDbMetrics(
    "get_assign_ids_for_offsets"
  )
  val getUnassingIdsForOffsets: DatabaseMetrics = createDbMetrics(
    "get_unassign_ids_for_offsets"
  )
  val getCreateIdsForContractIds: DatabaseMetrics = createDbMetrics(
    "get_create_ids_for_contract_ids"
  )
  val getAssignIdsForContractIds: DatabaseMetrics = createDbMetrics(
    "get_assign_ids_for_contract_ids"
  )

  val firstSynchronizerOffsetAfterOrAt: DatabaseMetrics = createDbMetrics(
    "first_synchronizer_offset_after_or_at"
  )
  val lastSynchronizerOffsetBeforeOrAt: DatabaseMetrics = createDbMetrics(
    "last_synchronizer_offset_before_or_at"
  )
  val synchronizerOffset: DatabaseMetrics = createDbMetrics("synchronizer_offset")
  val firstSynchronizerOffsetAfterOrAtPublicationTime: DatabaseMetrics = createDbMetrics(
    "first_synchronizer_offset_after_or_at_publication_time"
  )
  val lastSynchronizerOffsetBeforeOrAtPublicationTime: DatabaseMetrics = createDbMetrics(
    "last_synchronizer_offset_before_or_at_publication_time"
  )

  val archivals: DatabaseMetrics = createDbMetrics("archivals")

  object translation {
    val getLfPackage: Timer = openTelemetryMetricsFactory.timer(inventory.getLfPackage.info)
  }

  object compression {

    val createArgumentCompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.createArgumentCompressed.info)

    val createArgumentUncompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.createArgumentCompressed.info)

    val createKeyValueCompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.createKeyValueCompressed.info)

    val createKeyValueUncompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.createKeyValueUncompressed.info)

    val exerciseArgumentCompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.exerciseArgumentCompressed.info)

    val exerciseArgumentUncompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.exerciseArgumentUncompressed.info)

    val exerciseResultCompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.exerciseResultCompressed.info)

    val exerciseResultUncompressed: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.exerciseResultUncompressed.info)
  }

  object threadpool {
    private val prefix: MetricName = MainIndexDBMetrics.this.prefix :+ "threadpool"

    val connection: MetricName = prefix :+ "connection"
  }
}
