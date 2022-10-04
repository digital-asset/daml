// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Histogram, Timer}

import com.codahale.metrics.MetricRegistry

class IndexDBMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.FactoryWithDBMetrics {

  val storePartyEntry: Timer = timer(
    prefix :+ "store_party_entry"
  ) // FIXME: remove this metric variable since is is used only in Spec
  val storePackageEntry: Timer = timer(
    prefix :+ "store_package_entry"
  ) // FIXME: remove this metric variable since is is used only in Spec

  val storeTransactionCombined: Timer =
    timer(prefix :+ "store_ledger_entry_combined")

  val storeRejection: Timer = timer(
    prefix :+ "store_rejection"
  ) // FIXME: remove this metric variable since is is used only in Spec
  val storeConfigurationEntry: Timer = timer(
    prefix :+ "store_configuration_entry"
  ) // FIXME: remove this metric variable since is is used only in Spec

  val lookupLedgerId: Timer = timer(prefix :+ "lookup_ledger_id")
  val lookupParticipantId: Timer = timer(prefix :+ "lookup_participant_id")
  val lookupLedgerEnd: Timer = timer(prefix :+ "lookup_ledger_end")
  val lookupLedgerConfiguration: Timer =
    timer(prefix :+ "lookup_ledger_configuration")
  val lookupKey: Timer = timer(prefix :+ "lookup_key")
  val lookupActiveContract: Timer = timer(prefix :+ "lookup_active_contract")
  val getParties: Timer = timer(prefix :+ "get_parties")
  val listKnownParties: Timer = timer(prefix :+ "list_known_parties")
  val listLfPackages: Timer = timer(prefix :+ "list_lf_packages")
  val getLfArchive: Timer = timer(prefix :+ "get_lf_archive")
  val prune: Timer = timer(prefix :+ "prune")

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
    val getLfPackage: Timer = timer(prefix :+ "get_lf_package")
  }

  object compression {
    private val prefix: MetricName = IndexDBMetrics.this.prefix :+ "compression"

    val createArgumentCompressed: Histogram = histogram(prefix :+ "create_argument_compressed")
    val createArgumentUncompressed: Histogram = histogram(prefix :+ "create_argument_uncompressed")
    val createKeyValueCompressed: Histogram = histogram(prefix :+ "create_key_value_compressed")
    val createKeyValueUncompressed: Histogram = histogram(
      prefix :+ "create_key_value_uncompressed"
    )
    val exerciseArgumentCompressed: Histogram = histogram(prefix :+ "exercise_argument_compressed")
    val exerciseArgumentUncompressed: Histogram = histogram(
      prefix :+ "exercise_argument_uncompressed"
    )
    val exerciseResultCompressed: Histogram = histogram(prefix :+ "exercise_result_compressed")
    val exerciseResultUncompressed: Histogram = histogram(prefix :+ "exercise_result_uncompressed")
  }

  object threadpool {
    private val prefix: MetricName = IndexDBMetrics.this.prefix :+ "threadpool"

    val connection: MetricName = prefix :+ "connection"
  }
}
