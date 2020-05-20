// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._

class Metrics(val registry: MetricRegistry) {

  private def gauge[T](name: MetricName, metricSupplier: MetricSupplier[Gauge[_]]): Gauge[T] = {
    registry.remove(name)
    registry.gauge(name, metricSupplier).asInstanceOf[Gauge[T]]
  }

  object daml {
    val prefix: MetricName = MetricName.DAML

    object commands {
      val prefix: MetricName = daml.prefix :+ "commands"

      val submissions: Timer = registry.timer(prefix :+ "submissions")
      val validation: Timer = registry.timer(prefix :+ "validation")

      val failedCommandInterpretations: Meter =
        registry.meter(prefix :+ "failed_command_interpretations")
      val deduplicatedCommands: Meter =
        registry.meter(prefix :+ "deduplicated_commands")
      val delayedSubmissions: Meter =
        registry.meter(prefix :+ "delayed_submissions")
      val validSubmissions: Meter =
        registry.meter(prefix :+ "valid_submissions")
    }

    object execution {
      val prefix: MetricName = daml.prefix :+ "execution"

      val lookupActiveContract: Timer = registry.timer(prefix :+ "lookup_active_contract")
      val lookupContractKey: Timer = registry.timer(prefix :+ "lookup_contract_key")
      val getLfPackage: Timer = registry.timer(prefix :+ "get_lf_package")
      val retry: Meter = registry.meter(prefix :+ "retry")
      val total: Timer = registry.timer(prefix :+ "total")

    }

    object kvutils {
      val prefix: MetricName = daml.prefix :+ "kvutils"

      object committer {
        val prefix: MetricName = kvutils.prefix :+ "committer"

        // Timer (and count) of how fast submissions have been processed.
        val runTimer: Timer = registry.timer(prefix :+ "run_timer")

        // Number of exceptions seen.
        val exceptions: Counter = registry.counter(prefix :+ "exceptions")

        // Counter to monitor how many at a time and when kvutils is processing a submission.
        val processing: Counter = registry.counter(prefix :+ "processing")

        def runTimer(committerName: String): Timer =
          registry.timer(prefix :+ committerName :+ "run_timer")
        def stepTimer(committerName: String, stepName: String): Timer =
          registry.timer(prefix :+ committerName :+ "step_timers" :+ stepName)

        object last {
          val prefix: MetricName = committer.prefix :+ "last"

          val lastRecordTimeGauge = new VarGauge[String]("<none>")
          registry.register(prefix :+ "record_time", lastRecordTimeGauge)

          val lastEntryIdGauge = new VarGauge[String]("<none>")
          registry.register(prefix :+ "entry_id", lastEntryIdGauge)

          val lastParticipantIdGauge = new VarGauge[String]("<none>")
          registry.register(prefix :+ "participant_id", lastParticipantIdGauge)

          val lastExceptionGauge = new VarGauge[String]("<none>")
          registry.register(prefix :+ "exception", lastExceptionGauge)
        }

        object config {
          val prefix: MetricName = committer.prefix :+ "config"

          val accepts: Counter = registry.counter(prefix :+ "accepts")
          val rejections: Counter = registry.counter(prefix :+ "rejections")
        }

        object packageUpload {
          val prefix: MetricName = committer.prefix :+ "package_upload"

          val preloadTimer: Timer = registry.timer(prefix :+ "preload_timer")
          val decodeTimer: Timer = registry.timer(prefix :+ "decode_timer")
          val accepts: Counter = registry.counter(prefix :+ "accepts")
          val rejections: Counter = registry.counter(prefix :+ "rejections")
          def loadedPackages(value: () => Int): Gauge[Nothing] = {
            gauge(prefix :+ "loaded_packages", () => () => value())
          }
        }

        object partyAllocation {
          val prefix: MetricName = committer.prefix :+ "party_allocation"

          val accepts: Counter = registry.counter(prefix :+ "accepts")
          val rejections: Counter = registry.counter(prefix :+ "rejections")
        }

        object transaction {
          val prefix: MetricName = committer.prefix :+ "transaction"

          val runTimer: Timer = registry.timer(prefix :+ "run_timer")
          val interpretTimer: Timer = registry.timer(prefix :+ "interpret_timer")
          val accepts: Counter = registry.counter(prefix :+ "accepts")

          def rejection(name: String): Counter =
            registry.counter(prefix :+ s"rejections_$name")
        }
      }

      object reader {
        val prefix: MetricName = kvutils.prefix :+ "reader"

        val openEnvelope: Timer = registry.timer(prefix :+ "open_envelope")
        val parseUpdates: Timer = registry.timer(prefix :+ "parse_updates")
      }
      object submission {
        val prefix: MetricName = kvutils.prefix :+ "submission"

        object conversion {
          val prefix: MetricName = submission.prefix :+ "conversion"

          val transactionOutputs: Timer =
            registry.timer(prefix :+ "transaction_outputs")
          val transactionToSubmission: Timer =
            registry.timer(prefix :+ "transaction_to_submission")
          val archivesToSubmission: Timer =
            registry.timer(prefix :+ "archives_to_submission")
          val partyToSubmission: Timer =
            registry.timer(prefix :+ "party_to_submission")
          val configurationToSubmission: Timer =
            registry.timer(prefix :+ "configuration_to_submission")
        }

        object validator {
          val prefix: MetricName = submission.prefix :+ "validator"

          val openEnvelope: Timer = registry.timer(prefix :+ "open_envelope")
          val acquireTransactionLock: Timer = registry.timer(prefix :+ "acquire_transaction_lock")
          val failedToAcquireTransaction: Timer =
            registry.timer(prefix :+ "failed_to_acquire_transaction")
          val releaseTransactionLock: Timer = registry.timer(prefix :+ "release_transaction_lock")
          val validateSubmission: Timer = registry.timer(prefix :+ "validate_submission")
          val processSubmission: Timer = registry.timer(prefix :+ "process_submission")
          val commitSubmission: Timer = registry.timer(prefix :+ "commit_submission")
          val transformSubmission: Timer = registry.timer(prefix :+ "transform_submission")

          val stateValueCache = new CacheMetrics(registry, prefix :+ "state_value_cache")
        }
      }

      object writer {
        val prefix: MetricName = kvutils.prefix :+ "writer"

        val commit: Timer = registry.timer(prefix :+ "commit")
      }
    }

    object lapi {
      val prefix: MetricName = daml.prefix :+ "lapi"

      def forMethod(name: String): Timer = registry.timer(prefix :+ name)

    }

    object ledger {
      val prefix: MetricName = daml.prefix :+ "ledger"
      object database {
        val prefix: MetricName = ledger.prefix :+ "database"
        object queries {
          val prefix: MetricName = database.prefix :+ "queries"
          val selectLatestLogEntryId: Timer = registry.timer(prefix :+ "select_latest_log_entry_id")
          val selectFromLog: Timer = registry.timer(prefix :+ "select_from_log")
          val selectStateValuesByKeys: Timer =
            registry.timer(prefix :+ "select_state_values_by_keys")
          val updateOrRetrieveLedgerId: Timer =
            registry.timer(prefix :+ "update_or_retrieve_ledger_id")
          val insertRecordIntoLog: Timer = registry.timer(prefix :+ "insert_record_into_log")
          val updateState: Timer = registry.timer(prefix :+ "update_state")
          val truncate: Timer = registry.timer(prefix :+ "truncate")
        }
        object transactions {
          val prefix: MetricName = database.prefix :+ "transactions"

          def acquireConnection(name: String): Timer =
            registry.timer(prefix :+ name :+ "acquire_connection")
          def run(name: String): Timer =
            registry.timer(prefix :+ name :+ "run")
        }
      }
      object log {
        val prefix: MetricName = ledger.prefix :+ "log"

        val append: Timer = registry.timer(prefix :+ "append")
        val read: Timer = registry.timer(prefix :+ "read")
      }
      object state {
        val prefix: MetricName = ledger.prefix :+ "state"

        val read: Timer = registry.timer(prefix :+ "read")
        val write: Timer = registry.timer(prefix :+ "write")
      }
    }

    object index {
      private val prefix = daml.prefix :+ "index"

      val lookupContract: Timer = registry.timer(prefix :+ "lookup_contract")
      val lookupKey: Timer = registry.timer(prefix :+ "lookup_key")
      val lookupFlatTransactionById: Timer =
        registry.timer(prefix :+ "lookup_flat_transaction_by_id")
      val lookupTransactionTreeById: Timer =
        registry.timer(prefix :+ "lookup_transaction_tree_by_id")
      val lookupLedgerConfiguration: Timer = registry.timer(prefix :+ "lookup_ledger_configuration")
      val lookupMaximumLedgerTime: Timer = registry.timer(prefix :+ "lookup_maximum_ledger_time")
      val getParties: Timer = registry.timer(prefix :+ "get_parties")
      val listKnownParties: Timer = registry.timer(prefix :+ "list_known_parties")
      val listLfPackages: Timer = registry.timer(prefix :+ "list_lf_packages")
      val getLfArchive: Timer = registry.timer(prefix :+ "get_lf_archive")
      val getLfPackage: Timer = registry.timer(prefix :+ "get_lf_package")
      val deduplicateCommand: Timer = registry.timer(prefix :+ "deduplicate_command")
      val removeExpiredDeduplicationData: Timer =
        registry.timer(prefix :+ "remove_expired_deduplication_data")
      val stopDeduplicatingCommand: Timer =
        registry.timer(prefix :+ "stop_deduplicating_command")

      val publishTransaction: Timer = registry.timer(prefix :+ "publish_transaction")
      val publishPartyAllocation: Timer = registry.timer(prefix :+ "publish_party_allocation")
      val uploadPackages: Timer = registry.timer(prefix :+ "upload_packages")
      val publishConfiguration: Timer = registry.timer(prefix :+ "publish_configuration")

      // FIXME Name mushing and inconsistencies here, tracked by https://github.com/digital-asset/daml/issues/5926
      object db {

        val prefix: MetricName = index.prefix :+ "db"

        val storePartyEntry: Timer = registry.timer(prefix :+ "store_party_entry")
        val storeInitialState: Timer = registry.timer(prefix :+ "store_initial_state")
        val storePackageEntry: Timer = registry.timer(prefix :+ "store_package_entry")
        val storeTransaction: Timer = registry.timer(prefix :+ "store_ledger_entry")
        val storeRejection: Timer = registry.timer(prefix :+ "store_rejection")
        val storeConfigurationEntry: Timer = registry.timer(prefix :+ "store_configuration_entry")

        val lookupLedgerId: Timer = registry.timer(prefix :+ "lookup_ledger_id")
        val lookupLedgerEnd: Timer = registry.timer(prefix :+ "lookup_ledger_end")
        val lookupTransaction: Timer = registry.timer(prefix :+ "lookup_transaction")
        val lookupLedgerConfiguration: Timer =
          registry.timer(prefix :+ "lookup_ledger_configuration")
        val lookupKey: Timer = registry.timer(prefix :+ "lookup_key")
        val lookupActiveContract: Timer = registry.timer(prefix :+ "lookup_active_contract")
        val lookupMaximumLedgerTime: Timer = registry.timer(prefix :+ "lookup_maximum_ledger_time")
        val getParties: Timer = registry.timer(prefix :+ "get_parties")
        val listKnownParties: Timer = registry.timer(prefix :+ "list_known_parties")
        val listLfPackages: Timer = registry.timer(prefix :+ "list_lf_packages")
        val getLfArchive: Timer = registry.timer(prefix :+ "get_lf_archive")
        val deduplicateCommand: Timer = registry.timer(prefix :+ "deduplicate_command")
        val removeExpiredDeduplicationData: Timer =
          registry.timer(prefix :+ "remove_expired_deduplication_data")
        val stopDeduplicatingCommand: Timer =
          registry.timer(prefix :+ "stop_deduplicating_command")

        private val createDatabaseMetrics: String => DatabaseMetrics =
          DatabaseMetrics(registry, prefix)(_)

        private val overall = createDatabaseMetrics("all")
        val waitAll: Timer = overall.waitTimer
        val execAll: Timer = overall.executionTimer

        val getCompletions: DatabaseMetrics = createDatabaseMetrics("get_completions")
        val getLedgerId: DatabaseMetrics = createDatabaseMetrics("get_ledger_id")
        val getLedgerEnd: DatabaseMetrics = createDatabaseMetrics("get_ledger_end")
        val getInitialLedgerEnd: DatabaseMetrics = createDatabaseMetrics("get_initial_ledger_end")
        val initializeLedgerParameters: DatabaseMetrics = createDatabaseMetrics(
          "initialize_ledger_parameters")
        val lookupConfiguration: DatabaseMetrics = createDatabaseMetrics("lookup_configuration")
        val loadConfigurationEntries: DatabaseMetrics = createDatabaseMetrics(
          "load_configuration_entries")
        val storeConfigurationEntryDao: DatabaseMetrics = createDatabaseMetrics(
          "store_configuration_entry") // FIXME Base name conflicts with storeConfigurationEntry
        val storePartyEntryDao
          : DatabaseMetrics = createDatabaseMetrics("store_party_entry") // FIXME Base name conflicts with storePartyEntry
        val loadPartyEntries: DatabaseMetrics = createDatabaseMetrics("load_party_entries")
        val storeTransactionDao
          : DatabaseMetrics = createDatabaseMetrics("store_ledger_entry") // FIXME Base name conflicts with storeTransaction
        val storeRejectionDao
          : DatabaseMetrics = createDatabaseMetrics("store_rejection") // FIXME Base name conflicts with storeRejection
        val storeInitialStateFromScenario: DatabaseMetrics = createDatabaseMetrics(
          "store_initial_state_from_scenario")
        val loadParties: DatabaseMetrics = createDatabaseMetrics("load_parties")
        val loadAllParties: DatabaseMetrics = createDatabaseMetrics("load_all_parties")
        val loadPackages: DatabaseMetrics = createDatabaseMetrics("load_packages")
        val loadArchive: DatabaseMetrics = createDatabaseMetrics("load_archive")
        val storePackageEntryDao
          : DatabaseMetrics = createDatabaseMetrics("store_package_entry") // FIXME Base name conflicts with storePackageEntry
        val loadPackageEntries: DatabaseMetrics = createDatabaseMetrics("load_package_entries")
        val deduplicateCommandDao
          : DatabaseMetrics = createDatabaseMetrics("deduplicate_command") // FIXME Base name conflicts with deduplicateCommand
        val removeExpiredDeduplicationDataDao: DatabaseMetrics = createDatabaseMetrics(
          "remove_expired_deduplication_data") // FIXME Base name conflicts with removeExpiredDeduplicationData
        val stopDeduplicatingCommandDao: DatabaseMetrics = createDatabaseMetrics(
          "stop_deduplicating_command") // FIXME Base name conflicts with stopDeduplicatingCommand
        val truncateAllTables: DatabaseMetrics = createDatabaseMetrics("truncate_all_tables")
        val lookupActiveContractDao: DatabaseMetrics = createDatabaseMetrics(
          "lookup_active_contract") // FIXME Base name conflicts with lookupActiveContract
        val lookupContractByKey: DatabaseMetrics = createDatabaseMetrics("lookup_contract_by_key")
        val lookupMaximumLedgerTimeDao: DatabaseMetrics = createDatabaseMetrics(
          "lookup_maximum_ledger_time") // FIXME Base name conflicts with lookupActiveContract
        val getFlatTransactions: DatabaseMetrics = createDatabaseMetrics("get_flat_transactions")
        val lookupFlatTransactionById: DatabaseMetrics = createDatabaseMetrics(
          "lookup_flat_transaction_by_id")
        val getTransactionTrees: DatabaseMetrics = createDatabaseMetrics("get_transaction_trees")
        val lookupTransactionTreeById: DatabaseMetrics = createDatabaseMetrics(
          "lookup_transaction_tree_by_id")
        val getActiveContracts: DatabaseMetrics = createDatabaseMetrics("get_active_contracts")

        object translation {
          val prefix: MetricName = db.prefix :+ "translation"
          val cache = new CacheMetrics(registry, prefix :+ "cache")
        }

      }
    }
    object indexer {
      val prefix: MetricName = daml.prefix :+ "indexer"

      val lastReceivedRecordTime = new VarGauge[Long](0)
      registry.register(prefix :+ "last_received_record_time", lastReceivedRecordTime)

      val lastReceivedOffset = new VarGauge[String]("<none>")
      registry.register(prefix :+ "last_received_offset", lastReceivedOffset)

      def currentRecordTimeLag(value: () => Long): Gauge[Nothing] =
        gauge(prefix :+ "current_record_time_lag", () => () => value())

      val stateUpdateProcessing: Timer = registry.timer(prefix :+ "processed_state_updates")
    }
    object services {
      val prefix: MetricName = daml.prefix :+ "services"

      object indexService {
        val prefix: MetricName = services.prefix :+ "index"

        val listLfPackages: Timer = registry.timer(prefix :+ "list_lf_packages")
        val getLfArchive: Timer = registry.timer(prefix :+ "get_lf_archive")
        val getLfPackage: Timer = registry.timer(prefix :+ "get_lf_package")
        val packageEntries: Timer = registry.timer(prefix :+ "package_entries")
        val getLedgerConfiguration: Timer = registry.timer(prefix :+ "get_ledger_configuration")
        val currentLedgerEnd: Timer = registry.timer(prefix :+ "current_ledger_end")
        val getCompletions: Timer = registry.timer(prefix :+ "get_completions")
        val transactions: Timer = registry.timer(prefix :+ "transactions")
        val transactionTrees: Timer = registry.timer(prefix :+ "transaction_trees")
        val getTransactionById: Timer = registry.timer(prefix :+ "get_transaction_by_id")
        val getTransactionTreeById: Timer = registry.timer(prefix :+ "get_transaction_tree_by_id")
        val getActiveContracts: Timer = registry.timer(prefix :+ "get_active_contracts")
        val lookupActiveContract: Timer = registry.timer(prefix :+ "lookup_active_contract")
        val lookupContractKey: Timer = registry.timer(prefix :+ "lookup_contract_key")
        val lookupMaximumLedgerTime: Timer = registry.timer(prefix :+ "lookup_maximum_ledger_time")
        val getLedgerId: Timer = registry.timer(prefix :+ "get_ledger_id")
        val getParticipantId: Timer = registry.timer(prefix :+ "get_participant_id")
        val getParties: Timer = registry.timer(prefix :+ "get_parties")
        val listKnownParties: Timer = registry.timer(prefix :+ "list_known_parties")
        val partyEntries: Timer = registry.timer(prefix :+ "party_entries")
        val lookupConfiguration: Timer = registry.timer(prefix :+ "lookup_configuration")
        val configurationEntries: Timer = registry.timer(prefix :+ "configuration_entries")
        val deduplicateCommand: Timer = registry.timer(prefix :+ "deduplicate_command")
        val stopDeduplicateCommand: Timer = registry.timer(prefix :+ "stop_deduplicating_command")
      }

      object read {
        val prefix: MetricName = services.prefix :+ "read"

        val getLedgerInitialConditions: Timer =
          registry.timer(prefix :+ "get_ledger_initial_conditions")
        val stateUpdates: Timer = registry.timer(prefix :+ "state_updates")
      }

      object write {
        val prefix: MetricName = services.prefix :+ "write"

        val submitTransaction: Timer = registry.timer(prefix :+ "submit_transaction")
        val uploadPackages: Timer = registry.timer(prefix :+ "upload_packages")
        val allocateParty: Timer = registry.timer(prefix :+ "allocate_party")
        val submitConfiguration: Timer = registry.timer(prefix :+ "submit_configuration")
      }
    }
  }
}
