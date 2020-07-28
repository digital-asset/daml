// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._

final class Metrics(val registry: MetricRegistry) {

  private def gauge[T](name: MetricName, metricSupplier: MetricSupplier[Gauge[_]]): Gauge[T] = {
    registry.remove(name)
    registry.gauge(name, metricSupplier).asInstanceOf[Gauge[T]]
  }

  object test {
    private val Prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(registry, Prefix, "db")
  }

  object daml {
    private val Prefix: MetricName = MetricName.DAML

    object commands {
      private val Prefix: MetricName = daml.Prefix :+ "commands"

      val validation: Timer = registry.timer(Prefix :+ "validation")
      val submissions: Timer = registry.timer(Prefix :+ "submissions")

      val failedCommandInterpretations: Meter =
        registry.meter(Prefix :+ "failed_command_interpretations")
      val deduplicatedCommands: Meter =
        registry.meter(Prefix :+ "deduplicated_commands")
      val delayedSubmissions: Meter =
        registry.meter(Prefix :+ "delayed_submissions")
      val validSubmissions: Meter =
        registry.meter(Prefix :+ "valid_submissions")
    }

    object execution {
      private val Prefix: MetricName = daml.Prefix :+ "execution"

      val lookupActiveContract: Timer = registry.timer(Prefix :+ "lookup_active_contract")
      val lookupActiveContractPerExecution: Timer =
        registry.timer(Prefix :+ "lookup_active_contract_per_execution")
      val lookupActiveContractCountPerExecution: Histogram =
        registry.histogram(Prefix :+ "lookup_active_contract_count_per_execution")
      val lookupContractKey: Timer = registry.timer(Prefix :+ "lookup_contract_key")
      val lookupContractKeyPerExecution: Timer =
        registry.timer(Prefix :+ "lookup_contract_key_per_execution")
      val lookupContractKeyCountPerExecution: Histogram =
        registry.histogram(Prefix :+ "lookup_contract_key_count_per_execution")
      val getLfPackage: Timer = registry.timer(Prefix :+ "get_lf_package")
      val retry: Meter = registry.meter(Prefix :+ "retry")
      val total: Timer = registry.timer(Prefix :+ "total")
    }

    object kvutils {
      private val Prefix: MetricName = daml.Prefix :+ "kvutils"

      object committer {
        private val Prefix: MetricName = kvutils.Prefix :+ "committer"

        // Timer (and count) of how fast submissions have been processed.
        val runTimer: Timer = registry.timer(Prefix :+ "run_timer")

        // Counter to monitor how many at a time and when kvutils is processing a submission.
        val processing: Counter = registry.counter(Prefix :+ "processing")

        def runTimer(committerName: String): Timer =
          registry.timer(Prefix :+ committerName :+ "run_timer")
        def preExecutionRunTimer(committerName: String): Timer =
          registry.timer(Prefix :+ committerName :+ "preexecution_run_timer")
        def stepTimer(committerName: String, stepName: String): Timer =
          registry.timer(Prefix :+ committerName :+ "step_timers" :+ stepName)

        object last {
          private val Prefix: MetricName = committer.Prefix :+ "last"

          val lastRecordTimeGauge = new VarGauge[String]("<none>")
          registry.register(Prefix :+ "record_time", lastRecordTimeGauge)

          val lastEntryIdGauge = new VarGauge[String]("<none>")
          registry.register(Prefix :+ "entry_id", lastEntryIdGauge)

          val lastParticipantIdGauge = new VarGauge[String]("<none>")
          registry.register(Prefix :+ "participant_id", lastParticipantIdGauge)

          val lastExceptionGauge = new VarGauge[String]("<none>")
          registry.register(Prefix :+ "exception", lastExceptionGauge)
        }

        object config {
          private val Prefix: MetricName = committer.Prefix :+ "config"

          val accepts: Counter = registry.counter(Prefix :+ "accepts")
          val rejections: Counter = registry.counter(Prefix :+ "rejections")
        }

        object packageUpload {
          private val Prefix: MetricName = committer.Prefix :+ "package_upload"

          val preloadTimer: Timer = registry.timer(Prefix :+ "preload_timer")
          val decodeTimer: Timer = registry.timer(Prefix :+ "decode_timer")
          val accepts: Counter = registry.counter(Prefix :+ "accepts")
          val rejections: Counter = registry.counter(Prefix :+ "rejections")
          def loadedPackages(value: () => Int): Gauge[Nothing] = {
            gauge(Prefix :+ "loaded_packages", () => () => value())
          }
        }

        object partyAllocation {
          private val Prefix: MetricName = committer.Prefix :+ "party_allocation"

          val accepts: Counter = registry.counter(Prefix :+ "accepts")
          val rejections: Counter = registry.counter(Prefix :+ "rejections")
        }

        object transaction {
          private val Prefix: MetricName = committer.Prefix :+ "transaction"

          val runTimer: Timer = registry.timer(Prefix :+ "run_timer")
          val interpretTimer: Timer = registry.timer(Prefix :+ "interpret_timer")
          val accepts: Counter = registry.counter(Prefix :+ "accepts")

          def rejection(name: String): Counter =
            registry.counter(Prefix :+ s"rejections_$name")
        }
      }

      object reader {
        private val Prefix: MetricName = kvutils.Prefix :+ "reader"

        val openEnvelope: Timer = registry.timer(Prefix :+ "open_envelope")
        val parseUpdates: Timer = registry.timer(Prefix :+ "parse_updates")
      }

      object submission {
        private val Prefix: MetricName = kvutils.Prefix :+ "submission"

        object conversion {
          private val Prefix: MetricName = submission.Prefix :+ "conversion"

          val transactionOutputs: Timer =
            registry.timer(Prefix :+ "transaction_outputs")
          val transactionToSubmission: Timer =
            registry.timer(Prefix :+ "transaction_to_submission")
          val archivesToSubmission: Timer =
            registry.timer(Prefix :+ "archives_to_submission")
          val partyToSubmission: Timer =
            registry.timer(Prefix :+ "party_to_submission")
          val configurationToSubmission: Timer =
            registry.timer(Prefix :+ "configuration_to_submission")
        }

        object validator {
          private val Prefix: MetricName = submission.Prefix :+ "validator"

          val openEnvelope: Timer = registry.timer(Prefix :+ "open_envelope")
          val fetchInputs: Timer = registry.timer(Prefix :+ "fetch_inputs")
          val validate: Timer = registry.timer(Prefix :+ "validate")
          val commit: Timer = registry.timer(Prefix :+ "commit")
          val transformSubmission: Timer = registry.timer(Prefix :+ "transform_submission")

          val acquireTransactionLock: Timer = registry.timer(Prefix :+ "acquire_transaction_lock")
          val failedToAcquireTransaction: Timer =
            registry.timer(Prefix :+ "failed_to_acquire_transaction")
          val releaseTransactionLock: Timer = registry.timer(Prefix :+ "release_transaction_lock")

          val stateValueCache = new CacheMetrics(registry, Prefix :+ "state_value_cache")

          // The below metrics are only generated during parallel validation.
          // The counters track how many submissions we're processing in parallel.
          val batchSizes: Histogram = registry.histogram(Prefix :+ "batch_sizes")
          val receivedBatchSubmissionBytes: Histogram =
            registry.histogram(Prefix :+ "received_batch_submission_bytes")
          val receivedSubmissionBytes: Histogram =
            registry.histogram(Prefix :+ "received_submission_bytes")

          val validateAndCommit: Timer = registry.timer(Prefix :+ "validate_and_commit")
          val decode: Timer = registry.timer(Prefix :+ "decode")
          val detectConflicts: Timer = registry.timer(Prefix :+ "detect_conflicts")

          val decodeRunning: Counter = registry.counter(Prefix :+ "decode_running")
          val fetchInputsRunning: Counter = registry.counter(Prefix :+ "fetch_inputs_running")
          val validateRunning: Counter = registry.counter(Prefix :+ "validate_running")
          val commitRunning: Counter = registry.counter(Prefix :+ "commit_running")

          // The below metrics are only generated for pre-execution.
          val validatePreExecute: Timer = registry.timer(Prefix :+ "validate_pre_execute")
          val generateWriteSets: Timer = registry.timer(Prefix :+ "generate_write_sets")

          val validatePreExecuteRunning: Counter =
            registry.counter(Prefix :+ "validate_pre_execute_running")
        }
      }

      object writer {
        private val Prefix: MetricName = kvutils.Prefix :+ "writer"

        val commit: Timer = registry.timer(Prefix :+ "commit")

        val preExecutedCount: Counter = registry.counter(Prefix :+ "pre_executed_count")
        val preExecutedInterpretationCosts: Histogram =
          registry.histogram(Prefix :+ "pre_executed_interpretation_costs")
        val committedCount: Counter = registry.counter(Prefix :+ "committed_count")
        val committedInterpretationCosts: Histogram =
          registry.histogram(Prefix :+ "committed_interpretation_costs")
      }

      object conflictdetection {
        private val Prefix = kvutils.Prefix :+ "conflict_detection"

        val accepted: Counter =
          registry.counter(Prefix :+ "accepted")

        val conflicted: Counter =
          registry.counter(Prefix :+ "conflicted")

        val removedTransientKey: Counter =
          registry.counter(Prefix :+ "removed_transient_key")

        val recovered: Counter =
          registry.counter(Prefix :+ "recovered")

        val dropped: Counter =
          registry.counter(Prefix :+ "dropped")
      }
    }

    object lapi {
      private val Prefix: MetricName = daml.Prefix :+ "lapi"

      def forMethod(name: String): Timer = registry.timer(Prefix :+ name)
    }

    object ledger {
      private val Prefix: MetricName = daml.Prefix :+ "ledger"

      object database {
        private val Prefix: MetricName = ledger.Prefix :+ "database"

        object queries {
          private val Prefix: MetricName = database.Prefix :+ "queries"

          val selectLatestLogEntryId: Timer = registry.timer(Prefix :+ "select_latest_log_entry_id")
          val selectFromLog: Timer = registry.timer(Prefix :+ "select_from_log")
          val selectStateValuesByKeys: Timer =
            registry.timer(Prefix :+ "select_state_values_by_keys")
          val updateOrRetrieveLedgerId: Timer =
            registry.timer(Prefix :+ "update_or_retrieve_ledger_id")
          val insertRecordIntoLog: Timer = registry.timer(Prefix :+ "insert_record_into_log")
          val updateState: Timer = registry.timer(Prefix :+ "update_state")
          val truncate: Timer = registry.timer(Prefix :+ "truncate")
        }

        object transactions {
          private val Prefix: MetricName = database.Prefix :+ "transactions"

          def acquireConnection(name: String): Timer =
            registry.timer(Prefix :+ name :+ "acquire_connection")
          def run(name: String): Timer =
            registry.timer(Prefix :+ name :+ "run")
        }
      }

      object log {
        private val Prefix: MetricName = ledger.Prefix :+ "log"

        val append: Timer = registry.timer(Prefix :+ "append")
        val read: Timer = registry.timer(Prefix :+ "read")
      }

      object state {
        private val Prefix: MetricName = ledger.Prefix :+ "state"

        val read: Timer = registry.timer(Prefix :+ "read")
        val write: Timer = registry.timer(Prefix :+ "write")
      }
    }

    object index {
      private val Prefix = daml.Prefix :+ "index"

      val lookupContract: Timer = registry.timer(Prefix :+ "lookup_contract")
      val lookupKey: Timer = registry.timer(Prefix :+ "lookup_key")
      val lookupFlatTransactionById: Timer =
        registry.timer(Prefix :+ "lookup_flat_transaction_by_id")
      val lookupTransactionTreeById: Timer =
        registry.timer(Prefix :+ "lookup_transaction_tree_by_id")
      val lookupLedgerConfiguration: Timer = registry.timer(Prefix :+ "lookup_ledger_configuration")
      val lookupMaximumLedgerTime: Timer = registry.timer(Prefix :+ "lookup_maximum_ledger_time")
      val getParties: Timer = registry.timer(Prefix :+ "get_parties")
      val listKnownParties: Timer = registry.timer(Prefix :+ "list_known_parties")
      val listLfPackages: Timer = registry.timer(Prefix :+ "list_lf_packages")
      val getLfArchive: Timer = registry.timer(Prefix :+ "get_lf_archive")
      val getLfPackage: Timer = registry.timer(Prefix :+ "get_lf_package")
      val deduplicateCommand: Timer = registry.timer(Prefix :+ "deduplicate_command")
      val removeExpiredDeduplicationData: Timer =
        registry.timer(Prefix :+ "remove_expired_deduplication_data")
      val stopDeduplicatingCommand: Timer =
        registry.timer(Prefix :+ "stop_deduplicating_command")

      val publishTransaction: Timer = registry.timer(Prefix :+ "publish_transaction")
      val publishPartyAllocation: Timer = registry.timer(Prefix :+ "publish_party_allocation")
      val uploadPackages: Timer = registry.timer(Prefix :+ "upload_packages")
      val publishConfiguration: Timer = registry.timer(Prefix :+ "publish_configuration")

      // FIXME Name mushing and inconsistencies here, tracked by https://github.com/digital-asset/daml/issues/5926
      object db {
        private val Prefix: MetricName = index.Prefix :+ "db"

        val storePartyEntry: Timer = registry.timer(Prefix :+ "store_party_entry")
        val storeInitialState: Timer = registry.timer(Prefix :+ "store_initial_state")
        val storePackageEntry: Timer = registry.timer(Prefix :+ "store_package_entry")
        val storeTransaction: Timer = registry.timer(Prefix :+ "store_ledger_entry")
        val storeRejection: Timer = registry.timer(Prefix :+ "store_rejection")
        val storeConfigurationEntry: Timer = registry.timer(Prefix :+ "store_configuration_entry")

        val lookupLedgerId: Timer = registry.timer(Prefix :+ "lookup_ledger_id")
        val lookupLedgerEnd: Timer = registry.timer(Prefix :+ "lookup_ledger_end")
        val lookupTransaction: Timer = registry.timer(Prefix :+ "lookup_transaction")
        val lookupLedgerConfiguration: Timer =
          registry.timer(Prefix :+ "lookup_ledger_configuration")
        val lookupKey: Timer = registry.timer(Prefix :+ "lookup_key")
        val lookupActiveContract: Timer = registry.timer(Prefix :+ "lookup_active_contract")
        val lookupMaximumLedgerTime: Timer = registry.timer(Prefix :+ "lookup_maximum_ledger_time")
        val getParties: Timer = registry.timer(Prefix :+ "get_parties")
        val listKnownParties: Timer = registry.timer(Prefix :+ "list_known_parties")
        val listLfPackages: Timer = registry.timer(Prefix :+ "list_lf_packages")
        val getLfArchive: Timer = registry.timer(Prefix :+ "get_lf_archive")
        val deduplicateCommand: Timer = registry.timer(Prefix :+ "deduplicate_command")
        val removeExpiredDeduplicationData: Timer =
          registry.timer(Prefix :+ "remove_expired_deduplication_data")
        val stopDeduplicatingCommand: Timer =
          registry.timer(Prefix :+ "stop_deduplicating_command")

        private val createDbMetrics: String => DatabaseMetrics =
          new DatabaseMetrics(registry, Prefix, _)

        private val overall = createDbMetrics("all")
        val waitAll: Timer = overall.waitTimer
        val execAll: Timer = overall.executionTimer

        val getCompletions: DatabaseMetrics = createDbMetrics("get_completions")
        val getLedgerId: DatabaseMetrics = createDbMetrics("get_ledger_id")
        val getLedgerEnd: DatabaseMetrics = createDbMetrics("get_ledger_end")
        val getInitialLedgerEnd: DatabaseMetrics = createDbMetrics("get_initial_ledger_end")
        val initializeLedgerParameters: DatabaseMetrics = createDbMetrics(
          "initialize_ledger_parameters")
        val lookupConfiguration: DatabaseMetrics = createDbMetrics("lookup_configuration")
        val loadConfigurationEntries: DatabaseMetrics = createDbMetrics(
          "load_configuration_entries")
        val storeConfigurationEntryDbMetrics: DatabaseMetrics = createDbMetrics(
          "store_configuration_entry") // FIXME Base name conflicts with storeConfigurationEntry
        val storePartyEntryDbMetrics
          : DatabaseMetrics = createDbMetrics("store_party_entry") // FIXME Base name conflicts with storePartyEntry
        val loadPartyEntries: DatabaseMetrics = createDbMetrics("load_party_entries")

        object storeTransactionDbMetrics
            extends DatabaseMetrics(registry, Prefix, "store_ledger_entry") {
          // outside of SQL transaction
          val prepareBatches: Timer = registry.timer(dbPrefix :+ "prepare_batches")

          // in order within SQL transaction
          val commitValidation: Timer = registry.timer(dbPrefix :+ "commit_validation")
          val eventsBatch: Timer = registry.timer(dbPrefix :+ "events_batch")
          val deleteContractWitnessesBatch: Timer =
            registry.timer(dbPrefix :+ "delete_contract_witnesses_batch")
          val deleteContractsBatch: Timer = registry.timer(dbPrefix :+ "delete_contracts_batch")
          val insertContractsBatch: Timer = registry.timer(dbPrefix :+ "insert_contracts_batch")
          val insertContractWitnessesBatch: Timer =
            registry.timer(dbPrefix :+ "insert_contract_witnesses_batch")

          val insertCompletion: Timer = registry.timer(dbPrefix :+ "insert_completion")
          val updateLedgerEnd: Timer = registry.timer(dbPrefix :+ "update_ledger_end")
        }
        val storeRejectionDbMetrics
          : DatabaseMetrics = createDbMetrics("store_rejection") // FIXME Base name conflicts with storeRejection
        val storeInitialStateFromScenario: DatabaseMetrics = createDbMetrics(
          "store_initial_state_from_scenario")
        val loadParties: DatabaseMetrics = createDbMetrics("load_parties")
        val loadAllParties: DatabaseMetrics = createDbMetrics("load_all_parties")
        val loadPackages: DatabaseMetrics = createDbMetrics("load_packages")
        val loadArchive: DatabaseMetrics = createDbMetrics("load_archive")
        val storePackageEntryDbMetrics
          : DatabaseMetrics = createDbMetrics("store_package_entry") // FIXME Base name conflicts with storePackageEntry
        val loadPackageEntries: DatabaseMetrics = createDbMetrics("load_package_entries")
        val deduplicateCommandDbMetrics
          : DatabaseMetrics = createDbMetrics("deduplicate_command") // FIXME Base name conflicts with deduplicateCommand
        val removeExpiredDeduplicationDataDbMetrics: DatabaseMetrics = createDbMetrics(
          "remove_expired_deduplication_data") // FIXME Base name conflicts with removeExpiredDeduplicationData
        val stopDeduplicatingCommandDbMetrics: DatabaseMetrics = createDbMetrics(
          "stop_deduplicating_command") // FIXME Base name conflicts with stopDeduplicatingCommand
        val truncateAllTables: DatabaseMetrics = createDbMetrics("truncate_all_tables")
        val lookupActiveContractDbMetrics: DatabaseMetrics = createDbMetrics(
          "lookup_active_contract") // FIXME Base name conflicts with lookupActiveContract
        val lookupActiveContractWithCachedArgumentDbMetrics: DatabaseMetrics = createDbMetrics(
          "lookup_active_contract_with_cached_argument")
        val lookupContractByKey: DatabaseMetrics = createDbMetrics("lookup_contract_by_key")
        val lookupMaximumLedgerTimeDbMetrics: DatabaseMetrics = createDbMetrics(
          "lookup_maximum_ledger_time") // FIXME Base name conflicts with lookupActiveContract
        val getFlatTransactions: DatabaseMetrics = createDbMetrics("get_flat_transactions")
        val lookupFlatTransactionById: DatabaseMetrics = createDbMetrics(
          "lookup_flat_transaction_by_id")
        val getTransactionTrees: DatabaseMetrics = createDbMetrics("get_transaction_trees")
        val lookupTransactionTreeById: DatabaseMetrics = createDbMetrics(
          "lookup_transaction_tree_by_id")
        val getActiveContracts: DatabaseMetrics = createDbMetrics("get_active_contracts")
        val getEventSeqIdRange: DatabaseMetrics = createDbMetrics("get_event_sequential_id_range")
        val getAcsEventSeqIdRange: DatabaseMetrics =
          createDbMetrics("get_acs_event_sequential_id_range")

        object translation {
          private val Prefix: MetricName = db.Prefix :+ "translation"
          val cache = new CacheMetrics(registry, Prefix :+ "cache")
        }
      }
    }

    object indexer {
      private val Prefix: MetricName = daml.Prefix :+ "indexer"

      val lastReceivedRecordTime = new VarGauge[Long](0)
      registry.register(Prefix :+ "last_received_record_time", lastReceivedRecordTime)

      val lastReceivedOffset = new VarGauge[String]("<none>")
      registry.register(Prefix :+ "last_received_offset", lastReceivedOffset)

      def currentRecordTimeLag(value: () => Long): Gauge[Nothing] =
        gauge(Prefix :+ "current_record_time_lag", () => () => value())

      val stateUpdateProcessing: Timer = registry.timer(Prefix :+ "processed_state_updates")
    }

    object services {
      private val Prefix: MetricName = daml.Prefix :+ "services"

      object index {
        private val Prefix: MetricName = services.Prefix :+ "index"

        val listLfPackages: Timer = registry.timer(Prefix :+ "list_lf_packages")
        val getLfArchive: Timer = registry.timer(Prefix :+ "get_lf_archive")
        val getLfPackage: Timer = registry.timer(Prefix :+ "get_lf_package")
        val packageEntries: Timer = registry.timer(Prefix :+ "package_entries")
        val getLedgerConfiguration: Timer = registry.timer(Prefix :+ "get_ledger_configuration")
        val currentLedgerEnd: Timer = registry.timer(Prefix :+ "current_ledger_end")
        val getCompletions: Timer = registry.timer(Prefix :+ "get_completions")
        val transactions: Timer = registry.timer(Prefix :+ "transactions")
        val transactionTrees: Timer = registry.timer(Prefix :+ "transaction_trees")
        val getTransactionById: Timer = registry.timer(Prefix :+ "get_transaction_by_id")
        val getTransactionTreeById: Timer = registry.timer(Prefix :+ "get_transaction_tree_by_id")
        val getActiveContracts: Timer = registry.timer(Prefix :+ "get_active_contracts")
        val lookupActiveContract: Timer = registry.timer(Prefix :+ "lookup_active_contract")
        val lookupContractKey: Timer = registry.timer(Prefix :+ "lookup_contract_key")
        val lookupMaximumLedgerTime: Timer = registry.timer(Prefix :+ "lookup_maximum_ledger_time")
        val getLedgerId: Timer = registry.timer(Prefix :+ "get_ledger_id")
        val getParticipantId: Timer = registry.timer(Prefix :+ "get_participant_id")
        val getParties: Timer = registry.timer(Prefix :+ "get_parties")
        val listKnownParties: Timer = registry.timer(Prefix :+ "list_known_parties")
        val partyEntries: Timer = registry.timer(Prefix :+ "party_entries")
        val lookupConfiguration: Timer = registry.timer(Prefix :+ "lookup_configuration")
        val configurationEntries: Timer = registry.timer(Prefix :+ "configuration_entries")
        val deduplicateCommand: Timer = registry.timer(Prefix :+ "deduplicate_command")
        val stopDeduplicateCommand: Timer = registry.timer(Prefix :+ "stop_deduplicating_command")
      }

      object read {
        private val Prefix: MetricName = services.Prefix :+ "read"

        val getLedgerInitialConditions: Timer =
          registry.timer(Prefix :+ "get_ledger_initial_conditions")
        val stateUpdates: Timer = registry.timer(Prefix :+ "state_updates")
      }

      object write {
        private val Prefix: MetricName = services.Prefix :+ "write"

        val submitTransaction: Timer = registry.timer(Prefix :+ "submit_transaction")
        val uploadPackages: Timer = registry.timer(Prefix :+ "upload_packages")
        val allocateParty: Timer = registry.timer(Prefix :+ "allocate_party")
        val submitConfiguration: Timer = registry.timer(Prefix :+ "submit_configuration")
      }
    }
  }
}
