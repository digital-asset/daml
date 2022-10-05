// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Counter, Histogram, Meter, Timer}

import com.codahale.metrics.MetricRegistry

class ServicesMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {

  object index extends MetricHandle.Factory {
    override val prefix: MetricName = ServicesMetrics.this.prefix :+ "index"
    override val registry: MetricRegistry = ServicesMetrics.this.registry

    val listLfPackages: Timer = timer(prefix :+ "list_lf_packages")
    val getLfArchive: Timer = timer(prefix :+ "get_lf_archive")
    val packageEntries: Timer = timer(prefix :+ "package_entries")
    val getLedgerConfiguration: Timer = timer(prefix :+ "get_ledger_configuration")
    val currentLedgerEnd: Timer = timer(prefix :+ "current_ledger_end")
    val getCompletions: Timer = timer(prefix :+ "get_completions")
    val getCompletionsLimited: Timer = timer(prefix :+ "get_completions_limited")
    val transactions: Timer = timer(prefix :+ "transactions")
    val transactionTrees: Timer = timer(prefix :+ "transaction_trees")
    val getTransactionById: Timer = timer(prefix :+ "get_transaction_by_id")
    val getTransactionTreeById: Timer = timer(prefix :+ "get_transaction_tree_by_id")
    val getActiveContracts: Timer = timer(prefix :+ "get_active_contracts")
    val lookupActiveContract: Timer = timer(prefix :+ "lookup_active_contract")
    val lookupContractStateWithoutDivulgence: Timer = timer(
      prefix :+ "lookup_contract_state_without_divulgence"
    )
    val lookupContractKey: Timer = timer(prefix :+ "lookup_contract_key")
    val lookupMaximumLedgerTime: Timer = timer(prefix :+ "lookup_maximum_ledger_time")
    val getParticipantId: Timer = timer(prefix :+ "get_participant_id")
    val getParties: Timer = timer(prefix :+ "get_parties")
    val listKnownParties: Timer = timer(prefix :+ "list_known_parties")
    val partyEntries: Timer = timer(prefix :+ "party_entries")
    val lookupConfiguration: Timer = timer(prefix :+ "lookup_configuration")
    val configurationEntries: Timer = timer(prefix :+ "configuration_entries")
    val prune: Timer = timer(prefix :+ "prune")
    val getTransactionMetering: Timer = timer(prefix :+ "get_transaction_metering")

    object InMemoryFanoutBuffer extends MetricHandle.Factory {
      override val prefix: MetricName = index.prefix :+ "in_memory_fan_out_buffer"
      override val registry: MetricRegistry = index.registry

      val push: Timer = timer(prefix :+ "push")
      val prune: Timer = timer(prefix :+ "prune")

      val bufferSize: Histogram = histogram(prefix :+ "size")
    }

    case class BufferedReader(streamName: String) extends MetricHandle.Factory {
      override val prefix: MetricName = index.prefix :+ s"${streamName}_buffer_reader"
      override val registry: MetricRegistry = index.registry

      val fetchedTotal: Counter = counter(prefix :+ "fetched_total")
      val fetchedBuffered: Counter = counter(prefix :+ "fetched_buffered")
      val fetchTimer: Timer = timer(prefix :+ "fetch")
      val conversion: Timer = timer(prefix :+ "conversion")
      val slice: Timer = timer(prefix :+ "slice")
      val sliceSize: Histogram = histogram(prefix :+ "slice_size")
    }
  }

  object read extends MetricHandle.Factory {
    override val prefix: MetricName = ServicesMetrics.this.prefix :+ "read"
    override val registry: MetricRegistry = index.registry

    val getLedgerInitialConditions: Timer = timer(prefix :+ "get_ledger_initial_conditions")
    val stateUpdates: Timer = timer(prefix :+ "state_updates")
  }

  object write extends MetricHandle.Factory {
    override val prefix: MetricName = ServicesMetrics.this.prefix :+ "write"
    override val registry: MetricRegistry = index.registry

    val submitTransaction: Timer = timer(prefix :+ "submit_transaction")
    val submitTransactionRunning: Meter = meter(prefix :+ "submit_transaction_running")
    val uploadPackages: Timer = timer(prefix :+ "upload_packages")
    val allocateParty: Timer = timer(prefix :+ "allocate_party")
    val submitConfiguration: Timer = timer(prefix :+ "submit_configuration")
    val prune: Timer = timer(prefix :+ "prune")
  }
}
