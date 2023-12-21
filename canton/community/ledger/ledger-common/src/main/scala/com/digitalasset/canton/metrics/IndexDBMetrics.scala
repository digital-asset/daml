// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.metrics.api.{MetricDoc, MetricName}

import scala.annotation.nowarn

class IndexDBMetrics(
    val prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") val metricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends MainIndexDBMetrics(prefix, metricsFactory, labeledMetricsFactory)
    with TransactionStreamsDbMetrics

trait TransactionStreamsDbMetrics {
  self: DatabaseMetricsFactory =>
  val prefix: MetricName
  @deprecated("Use LabeledMetricsFactory", since = "2.7.0")
  val metricsFactory: MetricsFactory

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
    val translationTimer: Timer = metricsFactory.timer(prefix :+ "translation")
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
    val translationTimer: Timer = metricsFactory.timer(prefix :+ "translation")
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
    val translationTimer: Timer = metricsFactory.timer(prefix :+ "translation")
  }
}

class MainIndexDBMetrics(
    prefix: MetricName,
    @nowarn("cat=deprecation") metricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends DatabaseMetricsFactory(prefix, labeledMetricsFactory) { self =>

  @MetricDoc.Tag(
    summary = "The time spent looking up a contract using its key.",
    description = """This metric exposes the time spent looking up a contract using its key in the
                    |index db. It is then used by the Daml interpreter when evaluating a command
                    |into a transaction.""",
    qualification = Debug,
  )
  val lookupKey: Timer = metricsFactory.timer(prefix :+ "lookup_key")

  @MetricDoc.Tag(
    summary = "The time spent fetching a contract using its id.",
    description = """This metric exposes the time spent fetching a contract using its id from the
                    |index db. It is then used by the Daml interpreter when evaluating a command
                    |into a transaction.""",
    qualification = Debug,
  )
  val lookupActiveContract: Timer = metricsFactory.timer(prefix :+ "lookup_active_contract")

  @MetricDoc.Tag(
    summary = "The number of the currently pending active contract lookups.",
    description =
      "The number of the currently pending active contract lookups in the batch-loading queue of the Contract Service.",
    qualification = Debug,
  )
  val activeContractLookupBufferLength: Counter =
    metricsFactory.counter(prefix :+ "active_contract_lookup_buffer_length")

  @MetricDoc.Tag(
    summary = "The capacity of the active contract lookup queue.",
    description =
      """The maximum number of elements that can be kept in the queue of active contract lookups
        |in the batch-loading queue of the Contract Service.""",
    qualification = Debug,
  )
  val activeContractLookupBufferCapacity: Counter =
    metricsFactory.counter(prefix :+ "active_contract_lookup_buffer_capacity")

  @MetricDoc.Tag(
    summary = "The queuing delay for the active contract lookup queue.",
    description =
      "The queuing delay for the pending active contract lookups in the batch-loading queue of the Contract Service.",
    qualification = Debug,
  )
  val activeContractLookupBufferDelay: Timer =
    metricsFactory.timer(prefix :+ "active_contract_lookup_buffer_delay")

  @MetricDoc.Tag(
    summary = "The batch sizes in the active contract lookup batch-loading Contract Service.",
    description =
      """The number of active contract lookups contained in a batch, used in the batch-loading Contract Service.""",
    qualification = Debug,
  )
  val activeContractLookupBatchSize: Histogram =
    metricsFactory.histogram(prefix :+ "active_contract_lookup_batch_size")

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
    val getLfPackage: Timer = metricsFactory.timer(prefix :+ "get_lf_package")
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
    val createArgumentCompressed: Histogram =
      metricsFactory.histogram(prefix :+ "create_argument_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed argument of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed
                      |arguments of a create event.""",
      qualification = Debug,
    )
    val createArgumentUncompressed: Histogram =
      metricsFactory.histogram(prefix :+ "create_argument_uncompressed")

    @MetricDoc.Tag(
      summary = "The size of the compressed key value of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed key
                      |value of a create event.""",
      qualification = Debug,
    )
    val createKeyValueCompressed: Histogram =
      metricsFactory.histogram(prefix :+ "create_key_value_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed key value of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed key
                      |value of a create event.""",
      qualification = Debug,
    )
    val createKeyValueUncompressed: Histogram = metricsFactory.histogram(
      prefix :+ "create_key_value_uncompressed"
    )

    @MetricDoc.Tag(
      summary = "The size of the compressed argument of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |arguments of an exercise event.""",
      qualification = Debug,
    )
    val exerciseArgumentCompressed: Histogram =
      metricsFactory.histogram(prefix :+ "exercise_argument_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed argument of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed
                      |arguments of an exercise event.""",
      qualification = Debug,
    )
    val exerciseArgumentUncompressed: Histogram = metricsFactory.histogram(
      prefix :+ "exercise_argument_uncompressed"
    )

    @MetricDoc.Tag(
      summary = "The size of the compressed result of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |result of an exercise event.""",
      qualification = Debug,
    )
    val exerciseResultCompressed: Histogram =
      metricsFactory.histogram(prefix :+ "exercise_result_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed result of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |result of an exercise event.""",
      qualification = Debug,
    )
    val exerciseResultUncompressed: Histogram =
      metricsFactory.histogram(prefix :+ "exercise_result_uncompressed")
  }

  object threadpool {
    private val prefix: MetricName = MainIndexDBMetrics.this.prefix :+ "threadpool"

    val connection: MetricName = prefix :+ "connection"
  }
}
