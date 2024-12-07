// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{
  Counter,
  Histogram,
  LabeledMetricsFactory,
  MetricsFactory,
  Timer,
}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

import scala.annotation.nowarn

class IndexDBMetrics(
    val prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") val factory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends MainIndexDBMetrics(prefix, factory, labeledMetricsFactory)
    with TransactionStreamsDbMetrics {
  self =>
}

trait TransactionStreamsDbMetrics {
  self: DatabaseMetricsFactory =>
  val prefix: MetricName
  @deprecated("Use LabeledMetricsFactory", since = "2.7.0")
  val factory: MetricsFactory

  object flatTxStream {
    val prefix: MetricName = self.prefix :+ "flat_transactions_stream"

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
    @MetricDoc.Tag(
      summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
      description = """Some index database queries that target contracts and transactions involve a
                      |Daml-LF translation step. For such queries this metric stands for the time it
                      |takes to turn the serialized Daml-LF values into in-memory representation.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val translationTimer: Timer = factory.timer(prefix :+ "translation")
  }

  object treeTxStream {
    val prefix: MetricName = self.prefix :+ "tree_transactions_stream"

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
    @MetricDoc.Tag(
      summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
      description = """Some index database queries that target contracts and transactions involve a
                      |Daml-LF translation step. For such queries this metric stands for the time it
                      |takes to turn the serialized Daml-LF values into in-memory representation.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val translationTimer: Timer = factory.timer(prefix :+ "translation")
  }

  object reassignmentStream {
    val prefix: MetricName = self.prefix :+ "reassignment_stream"

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
    @MetricDoc.Tag(
      summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
      description = """Some index database queries that target contracts and transactions involve a
          |Daml-LF translation step. For such queries this metric stands for the time it
          |takes to turn the serialized Daml-LF values into in-memory representation.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val translationTimer: Timer = factory.timer(prefix :+ "translation")
  }
}

class BatchLoaderMetrics(
    parent: MetricName,
    labeledMetricsFactory: LabeledMetricsFactory,
) {

  private implicit val context: MetricsContext = MetricsContext.Empty
  private val prefix = parent :+ "batch.active_contract_lookup"

  @MetricDoc.Tag(
    summary = "The number of the currently pending active contract lookups.",
    description =
      "The number of the currently pending active contract lookups in the batch-loading queue of the Contract Service.",
    qualification = Debug,
  )
  val bufferLength: Counter =
    labeledMetricsFactory.counter(prefix :+ "buffer_length")

  @MetricDoc.Tag(
    summary = "The capacity of the lookup queue.",
    description = """The maximum number of elements that can be kept in the queue of lookups
        |in the batch-loading queue of the Contract Service.""",
    qualification = Debug,
  )
  val bufferCapacity: Counter =
    labeledMetricsFactory.counter(prefix :+ "buffer_capacity")

  @MetricDoc.Tag(
    summary = "The queuing delay for the lookup queue.",
    description =
      "The queuing delay for the pending lookups in the batch-loading queue of the Contract Service.",
    qualification = Debug,
  )
  val bufferDelay: Timer =
    labeledMetricsFactory.timer(prefix :+ "buffer_delay")

  @MetricDoc.Tag(
    summary = "The batch sizes in the lookup batch-loading Contract Service.",
    description =
      """The number of lookups contained in a batch, used in the batch-loading Contract Service.""",
    qualification = Debug,
  )
  val batchSize: Histogram =
    labeledMetricsFactory.histogram(prefix :+ "batch_size")

}

class MainIndexDBMetrics(
    prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") factory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends DatabaseMetricsFactory(prefix, labeledMetricsFactory) { self =>

  @MetricDoc.Tag(
    summary = "The time spent looking up a contract using its key.",
    description = """This metric exposes the time spent looking up a contract using its key in the
                    |index db. It is then used by the Daml interpreter when evaluating a command
                    |into a transaction.""",
    qualification = Debug,
  )
  @nowarn("cat=deprecation")
  val lookupKey: Timer = factory.timer(prefix :+ "lookup_key")

  @MetricDoc.Tag(
    summary = "The time spent fetching a contract using its id.",
    description = """This metric exposes the time spent fetching a contract using its id from the
                    |index db. It is then used by the Daml interpreter when evaluating a command
                    |into a transaction.""",
    qualification = Debug,
  )
  @nowarn("cat=deprecation")
  val lookupActiveContract: Timer = factory.timer(prefix :+ "lookup_active_contract")

  val activeContracts =
    new BatchLoaderMetrics(prefix :+ "active_contract_lookup", labeledMetricsFactory)
  val activeContractKeys =
    new BatchLoaderMetrics(prefix :+ "active_contract_keys_lookup", labeledMetricsFactory)

  private val overall = createDbMetrics("all")
  val waitAll: Timer = overall.waitTimer
  val execAll: Timer = overall.executionTimer

  val getCompletions: DatabaseMetrics = createDbMetrics("get_completions")
  val getLedgerId: DatabaseMetrics = createDbMetrics("get_ledger_id")
  val getParticipantId: DatabaseMetrics = createDbMetrics("get_participant_id")
  val getLedgerEnd: DatabaseMetrics = createDbMetrics("get_ledger_end")
  val initializeLedgerParameters: DatabaseMetrics = createDbMetrics(
    "initialize_ledger_parameters"
  )
  val lookupConfiguration: DatabaseMetrics = createDbMetrics("lookup_configuration")
  val loadConfigurationEntries: DatabaseMetrics = createDbMetrics(
    "load_configuration_entries"
  )
  val storeConfigurationEntryDbMetrics: DatabaseMetrics = createDbMetrics(
    "store_configuration_entry"
  )
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
  val loadPackages: DatabaseMetrics = createDbMetrics("load_packages")
  val loadArchive: DatabaseMetrics = createDbMetrics("load_archive")
  val storePackageEntryDbMetrics: DatabaseMetrics = createDbMetrics("store_package_entry")
  val loadPackageEntries: DatabaseMetrics = createDbMetrics("load_package_entries")
  val pruneDbMetrics: DatabaseMetrics = createDbMetrics("prune")
  val fetchPruningOffsetsMetrics: DatabaseMetrics = createDbMetrics("fetch_pruning_offsets")
  val lookupDivulgedActiveContractDbMetrics: DatabaseMetrics = createDbMetrics(
    "lookup_divulged_active_contract"
  )
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

  val lookupFlatTransactionById: DatabaseMetrics = createDbMetrics(
    "lookup_flat_transaction_by_id"
  )
  val lookupTransactionTreeById: DatabaseMetrics = createDbMetrics(
    "lookup_transaction_tree_by_id"
  )
  val getEventsByContractId: DatabaseMetrics = createDbMetrics("get_events_by_contract_id")
  val getEventsByContractKey: DatabaseMetrics = createDbMetrics("get_events_by_contract_key")
  val getEventSequentialIdForEventId: DatabaseMetrics = createDbMetrics(
    "get_event_sequential_id_for_event_id"
  )
  val getActiveContracts: DatabaseMetrics = createDbMetrics("get_active_contracts")
  val getActiveContractIdsForCreated: DatabaseMetrics = createDbMetrics(
    "get_active_contract_ids_for_created"
  )
  val getActiveContractIdsForAssigned: DatabaseMetrics = createDbMetrics(
    "get_active_contract_ids_for_assigned"
  )
  val getActiveContractBatchForNotArchived: DatabaseMetrics = createDbMetrics(
    "get_active_contract_batch_for_not_archived"
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

  object translation {
    private val prefix: MetricName = MainIndexDBMetrics.this.prefix :+ "translation"

    @MetricDoc.Tag(
      summary = "The time needed to deserialize and decode a Daml-LF archive.",
      description = """A Daml archive before it can be used in the interpretation needs to be
                      |deserialized and decoded, in other words converted into the in-memory
                      |representation. This metric represents time necessary to do that.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val getLfPackage: Timer = factory.timer(prefix :+ "get_lf_package")
  }

  object compression {
    private val prefix: MetricName = MainIndexDBMetrics.this.prefix :+ "compression"

    @MetricDoc.Tag(
      summary = "The size of the compressed arguments of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |arguments of a create event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val createArgumentCompressed: Histogram =
      factory.histogram(prefix :+ "create_argument_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed argument of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed
                      |arguments of a create event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val createArgumentUncompressed: Histogram =
      factory.histogram(prefix :+ "create_argument_uncompressed")

    @MetricDoc.Tag(
      summary = "The size of the compressed key value of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed key
                      |value of a create event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val createKeyValueCompressed: Histogram =
      factory.histogram(prefix :+ "create_key_value_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed key value of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed key
                      |value of a create event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val createKeyValueUncompressed: Histogram = factory.histogram(
      prefix :+ "create_key_value_uncompressed"
    )

    @MetricDoc.Tag(
      summary = "The size of the compressed argument of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |arguments of an exercise event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val exerciseArgumentCompressed: Histogram =
      factory.histogram(prefix :+ "exercise_argument_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed argument of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed
                      |arguments of an exercise event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val exerciseArgumentUncompressed: Histogram = factory.histogram(
      prefix :+ "exercise_argument_uncompressed"
    )

    @MetricDoc.Tag(
      summary = "The size of the compressed result of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |result of an exercise event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val exerciseResultCompressed: Histogram =
      factory.histogram(prefix :+ "exercise_result_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed result of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |result of an exercise event.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val exerciseResultUncompressed: Histogram =
      factory.histogram(prefix :+ "exercise_result_uncompressed")
  }

  object threadpool {
    private val prefix: MetricName = MainIndexDBMetrics.this.prefix :+ "threadpool"

    val connection: MetricName = prefix :+ "connection"

  }
}
