// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.{Histogram, Timer}

import com.codahale.metrics.MetricRegistry

class IndexDBMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.FactoryWithDBMetrics {

  @MetricDoc.Tag(
    summary = "The time spent looking up a contract using its key.",
    description = """This metric exposes the time spent looking up a contract using its key in the
                    |index db. It is then used by the Daml interpreter when evaluating a command
                    |into a transaction.""",
    qualification = Debug,
  )
  val lookupKey: Timer = timer(prefix :+ "lookup_key")

  @MetricDoc.Tag(
    summary = "The time spent fetching a contract using its id.",
    description = """This metric exposes the time spent fetching a contract using its id from the
                    |index db. It is then used by the Daml interpreter when evaluating a command
                    |into a transaction.""",
    qualification = Debug,
  )
  val lookupActiveContract: Timer = timer(prefix :+ "lookup_active_contract")

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

  object storeTransactionDbMetrics extends DatabaseMetrics(prefix, "store_ledger_entry", registry)

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
  val lookupActiveContractDbMetrics: DatabaseMetrics = createDbMetrics("lookup_active_contract")
  val lookupContractByKeyDbMetrics: DatabaseMetrics = createDbMetrics(
    "lookup_contract_by_key"
  )
  val getFlatTransactions: DatabaseMetrics = createDbMetrics("get_flat_transactions")
  val lookupFlatTransactionById: DatabaseMetrics = createDbMetrics(
    "lookup_flat_transaction_by_id"
  )
  val getTransactionTrees: DatabaseMetrics = createDbMetrics("get_transaction_trees")
  val lookupTransactionTreeById: DatabaseMetrics = createDbMetrics(
    "lookup_transaction_tree_by_id"
  )
  val getActiveContracts: DatabaseMetrics = createDbMetrics("get_active_contracts")
  val getActiveContractIds: DatabaseMetrics = createDbMetrics("get_active_contract_ids")
  val getActiveContractBatch: DatabaseMetrics = createDbMetrics("get_active_contract_batch")
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

  object translation {
    private val prefix: MetricName = IndexDBMetrics.this.prefix :+ "translation"

    @MetricDoc.Tag(
      summary = "The time needed to deserialize and decode a Daml-LF archive.",
      description = """A Daml archive before it can be used in the interpretation needs to be
                      |deserialized and decoded, in other words converted into the in-memory
                      |representation. This metric represents time necessary to do that.""",
      qualification = Debug,
    )
    val getLfPackage: Timer = timer(prefix :+ "get_lf_package")
  }

  object compression {
    private val prefix: MetricName = IndexDBMetrics.this.prefix :+ "compression"

    @MetricDoc.Tag(
      summary = "The size of the compressed arguments of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |arguments of a create event.""",
      qualification = Debug,
    )
    val createArgumentCompressed: Histogram = histogram(prefix :+ "create_argument_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed argument of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed
                      |arguments of a create event.""",
      qualification = Debug,
    )
    val createArgumentUncompressed: Histogram = histogram(prefix :+ "create_argument_uncompressed")

    @MetricDoc.Tag(
      summary = "The size of the compressed key value of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed key
                      |value of a create event.""",
      qualification = Debug,
    )
    val createKeyValueCompressed: Histogram = histogram(prefix :+ "create_key_value_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed key value of a create event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed key
                      |value of a create event.""",
      qualification = Debug,
    )
    val createKeyValueUncompressed: Histogram = histogram(
      prefix :+ "create_key_value_uncompressed"
    )

    @MetricDoc.Tag(
      summary = "The size of the compressed argument of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |arguments of an exercise event.""",
      qualification = Debug,
    )
    val exerciseArgumentCompressed: Histogram = histogram(prefix :+ "exercise_argument_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed argument of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of decompressed
                      |arguments of an exercise event.""",
      qualification = Debug,
    )
    val exerciseArgumentUncompressed: Histogram = histogram(
      prefix :+ "exercise_argument_uncompressed"
    )

    @MetricDoc.Tag(
      summary = "The size of the compressed result of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |result of an exercise event.""",
      qualification = Debug,
    )
    val exerciseResultCompressed: Histogram = histogram(prefix :+ "exercise_result_compressed")

    @MetricDoc.Tag(
      summary = "The size of the decompressed result of an exercise event.",
      description = """Event information can be compressed by the indexer before storing it in the
                      |database. This metric collects statistics about the size of compressed
                      |result of an exercise event.""",
      qualification = Debug,
    )
    val exerciseResultUncompressed: Histogram = histogram(prefix :+ "exercise_result_uncompressed")
  }

  object threadpool {
    private val prefix: MetricName = IndexDBMetrics.this.prefix :+ "threadpool"

    val connection: MetricName = prefix :+ "connection"

    val instrumentedExecutorServiceForDocs = new InstrumentedExecutorServiceForDocs(
      connection :+ "<server_role>"
    )
  }
}
