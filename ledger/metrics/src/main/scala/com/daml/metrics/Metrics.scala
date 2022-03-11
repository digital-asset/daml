// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.prometheus.client
import io.prometheus.client.cache.caffeine.CacheMetricsCollector
import com.github.benmanes.caffeine.{cache => caffeine}
import io.prometheus.client.Summary

final class Metrics() {

  private[metrics] val cacheMetricsCollector: CacheMetricsCollector =
    new CacheMetricsCollector().register[CacheMetricsCollector]()

  def registerCaffeineCache(cacheMetrics: CacheMetrics, caffeineCache: caffeine.Cache[_, _]): Unit =
    cacheMetricsCollector.addCache(cacheMetrics.cacheName, caffeineCache)

  object test {
    private val Prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(Prefix, "db")
  }

  object daml {
    private val Prefix: MetricName = MetricName.Daml

    object commands {
      private val Prefix: MetricName = daml.Prefix :+ "commands"

      val validation: client.Summary = summary(Prefix :+ "validation")
      val submissions: client.Summary = summary(Prefix :+ "submissions")
      val submissionsRunning: client.Gauge = gauge(Prefix :+ "submissions_running")

      val failedCommandInterpretations: client.Counter =
        counter(Prefix :+ "failed_command_interpretations")
      val delayedSubmissions: client.Counter =
        counter(Prefix :+ "delayed_submissions")
      val validSubmissions: client.Counter =
        counter(Prefix :+ "valid_submissions")

      val inputBufferLength: client.Gauge = gauge(Prefix :+ "input_buffer_length")
      val inputBufferCapacity: client.Gauge = gauge(Prefix :+ "input_buffer_capacity")
      val inputBufferDelay: client.Summary = summary(Prefix :+ "input_buffer_delay")
      val maxInFlightLength: client.Gauge = gauge(Prefix :+ "max_in_flight_length")
      val maxInFlightCapacity: client.Gauge = gauge(Prefix :+ "max_in_flight_capacity")
    }

    object execution {
      private val Prefix: MetricName = daml.Prefix :+ "execution"

      val lookupActiveContract: client.Summary = summary(Prefix :+ "lookup_active_contract")
      val lookupActiveContractPerExecution: client.Summary =
        summary(Prefix :+ "lookup_active_contract_per_execution")
      val lookupActiveContractCountPerExecution: client.Summary =
        summary(Prefix :+ "lookup_active_contract_count_per_execution")
      val lookupContractKey: client.Summary = summary(Prefix :+ "lookup_contract_key")
      val lookupContractKeyPerExecution: client.Summary =
        summary(Prefix :+ "lookup_contract_key_per_execution")
      val lookupContractKeyCountPerExecution: client.Summary =
        summary(Prefix :+ "lookup_contract_key_count_per_execution")
      val getLfPackage: client.Summary = summary(Prefix :+ "get_lf_package")
      val retry: client.Counter = counter(Prefix :+ "retry")

      // Total time for command execution (including data fetching)
      val total: client.Summary = summary(Prefix :+ "total")
      val totalRunning: client.Gauge = gauge(Prefix :+ "total_running")

      // Commands being executed by the engine (not currently fetching data)
      val engineRunning: client.Gauge = gauge(Prefix :+ "engine_running")

      object cache {
        private val Prefix: MetricName = execution.Prefix :+ "cache"

        val keyState: CacheMetrics = CacheMetrics(Prefix :+ "key_state")
        val contractState: CacheMetrics =
          CacheMetrics(Prefix :+ "contract_state")

        val registerCacheUpdate: client.Summary = summary(Prefix :+ "register_update")

        val dispatcherLag: client.Summary = summary(Prefix :+ "dispatcher_lag")

        val resolveDivulgenceLookup: client.Gauge =
          gauge(Prefix :+ "resolve_divulgence_lookup")

        val resolveFullLookup: client.Gauge =
          gauge(Prefix :+ "resolve_full_lookup")

        val readThroughNotFound: client.Gauge = gauge(Prefix :+ "read_through_not_found")

        // TODO Prometheus metrics: implement
        val indexSequentialId = new VarGauge[Long](0L)
//        registry.register(
//          Prefix :+ "index_sequential_id",
//          indexSequentialId,
//        )
      }
    }

    object kvutils {
      private val Prefix: MetricName = daml.Prefix :+ "kvutils"

      object committer {
        private val Prefix: MetricName = kvutils.Prefix :+ "committer"

        // Timer (and count) of how fast submissions have been processed.
        val runTimer: client.Summary = summary(Prefix :+ "run_timer")

        // Counter to monitor how many at a time and when kvutils is processing a submission.
        val processing: client.Gauge = gauge(Prefix :+ "processing")

        def runTimer(committerName: String): client.Summary = summary(
          Prefix :+ committerName :+ "run_timer"
        )
        def preExecutionRunTimer(committerName: String): client.Summary =
          summary(Prefix :+ committerName :+ "preexecution_run_timer")
        def stepTimer(committerName: String, stepName: String): client.Summary =
          summary(Prefix :+ committerName :+ "step_timers" :+ stepName)

        object last {
//          private val Prefix: MetricName = committer.Prefix :+ "last"

          // TODO Prometheus metrics: implement
          val lastRecordTimeGauge = new VarGauge[String]("<none>")
//          registry.register(Prefix :+ "record_time", lastRecordTimeGauge)
//
          val lastEntryIdGauge = new VarGauge[String]("<none>")
//          registry.register(Prefix :+ "entry_id", lastEntryIdGauge)
//
          val lastParticipantIdGauge = new VarGauge[String]("<none>")
//          registry.register(Prefix :+ "participant_id", lastParticipantIdGauge)
//
          val lastExceptionGauge = new VarGauge[String]("<none>")
//          registry.register(Prefix :+ "exception", lastExceptionGauge)
        }

        object config {
          private val Prefix: MetricName = committer.Prefix :+ "config"

          val accepts: client.Gauge = gauge(Prefix :+ "accepts")
          val rejections: client.Gauge = gauge(Prefix :+ "rejections")
        }

        object packageUpload {
          private val Prefix: MetricName = committer.Prefix :+ "package_upload"

          val validateTimer: client.Summary = summary(Prefix :+ "validate_timer")
          val preloadTimer: client.Summary = summary(Prefix :+ "preload_timer")
          val decodeTimer: client.Summary = summary(Prefix :+ "decode_timer")
          val accepts: client.Gauge = gauge(Prefix :+ "accepts")
          val rejections: client.Gauge = gauge(Prefix :+ "rejections")

          // TODO Prometheus cache: implement a replacement
//          def loadedPackages(value: () => Int): Unit = {
//            register(Prefix :+ "loaded_packages", () => () => value())
//          }
        }

        object partyAllocation {
          private val Prefix: MetricName = committer.Prefix :+ "party_allocation"

          val accepts: client.Gauge = gauge(Prefix :+ "accepts")
          val rejections: client.Gauge = gauge(Prefix :+ "rejections")
        }

        object transaction {
          private val Prefix: MetricName = committer.Prefix :+ "transaction"

          val runTimer: client.Summary = summary(Prefix :+ "run_timer")
          val interpretTimer: client.Summary = summary(Prefix :+ "interpret_timer")
          val accepts: client.Gauge = gauge(Prefix :+ "accepts")

          def rejection(name: String): client.Gauge =
            gauge(Prefix :+ s"rejections_$name")
        }
      }

      object reader {
        private val Prefix: MetricName = kvutils.Prefix :+ "reader"

        val openEnvelope: client.Summary = summary(Prefix :+ "open_envelope")
        val parseUpdates: client.Summary = summary(Prefix :+ "parse_updates")
      }

      object submission {
        private val Prefix: MetricName = kvutils.Prefix :+ "submission"

        object conversion {
          private val Prefix: MetricName = submission.Prefix :+ "conversion"

          val transactionOutputs: client.Summary =
            summary(Prefix :+ "transaction_outputs")
          val transactionToSubmission: client.Summary =
            summary(Prefix :+ "transaction_to_submission")
          val archivesToSubmission: client.Summary =
            summary(Prefix :+ "archives_to_submission")
          val partyToSubmission: client.Summary =
            summary(Prefix :+ "party_to_submission")
          val configurationToSubmission: client.Summary =
            summary(Prefix :+ "configuration_to_submission")
        }

        object validator {
          private val Prefix: MetricName = submission.Prefix :+ "validator"

          val openEnvelope: client.Summary = summary(Prefix :+ "open_envelope")
          val fetchInputs: client.Summary = summary(Prefix :+ "fetch_inputs")
          val validate: client.Summary = summary(Prefix :+ "validate")
          val commit: client.Summary = summary(Prefix :+ "commit")
          val transformSubmission: client.Summary = summary(Prefix :+ "transform_submission")

          val acquireTransactionLock: client.Summary = summary(Prefix :+ "acquire_transaction_lock")
          val failedToAcquireTransaction: client.Summary =
            summary(Prefix :+ "failed_to_acquire_transaction")
          val releaseTransactionLock: client.Summary = summary(Prefix :+ "release_transaction_lock")

          val stateValueCache = CacheMetrics(Prefix :+ "state_value_cache")

          // The below metrics are only generated during parallel validation.
          // The counters track how many submissions we're processing in parallel.
          val batchSizes: client.Summary = summary(Prefix :+ "batch_sizes")
          val receivedBatchSubmissionBytes: client.Summary =
            summary(Prefix :+ "received_batch_submission_bytes")
          val receivedSubmissionBytes: client.Summary =
            summary(Prefix :+ "received_submission_bytes")

          val validateAndCommit: client.Summary = summary(Prefix :+ "validate_and_commit")
          val decode: client.Summary = summary(Prefix :+ "decode")
          val detectConflicts: client.Summary = summary(Prefix :+ "detect_conflicts")

          val decodeRunning: client.Gauge = gauge(Prefix :+ "decode_running")
          val fetchInputsRunning: client.Gauge = gauge(Prefix :+ "fetch_inputs_running")
          val validateRunning: client.Gauge = gauge(Prefix :+ "validate_running")
          val commitRunning: client.Gauge = gauge(Prefix :+ "commit_running")

          // The below metrics are only generated for pre-execution.
          val validatePreExecute: client.Summary = summary(Prefix :+ "validate_pre_execute")
          val generateWriteSets: client.Summary = summary(Prefix :+ "generate_write_sets")

          val validatePreExecuteRunning: client.Gauge =
            gauge(Prefix :+ "validate_pre_execute_running")
        }
      }

      object writer {
        private val Prefix: MetricName = kvutils.Prefix :+ "writer"

        val commit: client.Summary = summary(Prefix :+ "commit")

        val preExecutedCount: client.Gauge = gauge(Prefix :+ "pre_executed_count")
        val preExecutedInterpretationCosts: client.Summary =
          summary(Prefix :+ "pre_executed_interpretation_costs")
        val committedCount: client.Gauge = gauge(Prefix :+ "committed_count")
        val committedInterpretationCosts: client.Summary =
          summary(Prefix :+ "committed_interpretation_costs")
      }

      object conflictdetection {
        private val Prefix = kvutils.Prefix :+ "conflict_detection"

        val accepted: client.Gauge =
          gauge(Prefix :+ "accepted")

        val conflicted: client.Gauge =
          gauge(Prefix :+ "conflicted")

        val removedTransientKey: client.Gauge =
          gauge(Prefix :+ "removed_transient_key")

        val recovered: client.Gauge =
          gauge(Prefix :+ "recovered")

        val dropped: client.Gauge =
          gauge(Prefix :+ "dropped")
      }
    }

    object lapi {
      private val Prefix: MetricName = daml.Prefix :+ "lapi"

      def forMethod(name: String): client.Summary = summary(Prefix :+ name)

      object threadpool {
        private val Prefix: MetricName = lapi.Prefix :+ "threadpool"

        val apiServices: MetricName = Prefix :+ "api-services"
      }

      object streams {
        private val Prefix: MetricName = lapi.Prefix :+ "streams"

        // TODO Prometheus metrics: change this to counters
        val transactionTrees: client.Gauge = gauge(Prefix :+ "transaction_trees_sent")
        val transactions: client.Gauge = gauge(Prefix :+ "transactions_sent")
        val completions: client.Gauge = gauge(Prefix :+ "completions_sent")
        val acs: client.Gauge = gauge(Prefix :+ "acs_sent")
      }
    }

    object ledger {
      private val Prefix: MetricName = daml.Prefix :+ "ledger"

      object database {
        private val Prefix: MetricName = ledger.Prefix :+ "database"

        object queries {
          private val Prefix: MetricName = database.Prefix :+ "queries"

          val selectLatestLogEntryId: client.Summary = summary(
            Prefix :+ "select_latest_log_entry_id"
          )
          val selectFromLog: client.Summary = summary(Prefix :+ "select_from_log")
          val selectStateValuesByKeys: client.Summary =
            summary(Prefix :+ "select_state_values_by_keys")
          val updateOrRetrieveLedgerId: client.Summary =
            summary(Prefix :+ "update_or_retrieve_ledger_id")
          val insertRecordIntoLog: client.Summary = summary(Prefix :+ "insert_record_into_log")
          val updateState: client.Summary = summary(Prefix :+ "update_state")
          val truncate: client.Summary = summary(Prefix :+ "truncate")
        }

        object transactions {
          private val Prefix: MetricName = database.Prefix :+ "transactions"

          def acquireConnection(name: String): client.Summary =
            summary(Prefix :+ name :+ "acquire_connection")
          def run(name: String): client.Summary =
            summary(Prefix :+ name :+ "run")
        }
      }

      object log {
        private val Prefix: MetricName = ledger.Prefix :+ "log"

        val append: client.Summary = summary(Prefix :+ "append")
        val read: client.Summary = summary(Prefix :+ "read")
      }

      object state {
        private val Prefix: MetricName = ledger.Prefix :+ "state"

        val read: client.Summary = summary(Prefix :+ "read")
        val write: client.Summary = summary(Prefix :+ "write")
      }
    }

    object userManagement {
      private val Prefix = daml.Prefix :+ "user_management"

      val cache = CacheMetrics(Prefix :+ "cache")

      private def createDbMetrics(name: String): DatabaseMetrics =
        new DatabaseMetrics(Prefix, name)
      val getUserInfo: DatabaseMetrics = createDbMetrics("get_user_info")
      val createUser: DatabaseMetrics = createDbMetrics("create_user")
      val deleteUser: DatabaseMetrics = createDbMetrics("delete_user")
      val grantRights: DatabaseMetrics = createDbMetrics("grant_rights")
      val revokeRights: DatabaseMetrics = createDbMetrics("revoke_rights")
      val listUsers: DatabaseMetrics = createDbMetrics("list_users")
    }
    object index {
      private val Prefix = daml.Prefix :+ "index"

      val decodeStateEvent: client.Summary = summary(Prefix :+ "decode_state_event")
      val lookupContract: client.Summary = summary(Prefix :+ "lookup_contract")
      val lookupKey: client.Summary = summary(Prefix :+ "lookup_key")
      val lookupFlatTransactionById: client.Summary =
        summary(Prefix :+ "lookup_flat_transaction_by_id")
      val lookupTransactionTreeById: client.Summary =
        summary(Prefix :+ "lookup_transaction_tree_by_id")
      val lookupLedgerConfiguration: client.Summary = summaryWithLabel(
        Prefix :+ "lookup_ledger_configuration"
      )
      val lookupMaximumLedgerTime: client.Summary = summaryWithLabel(
        Prefix :+ "lookup_maximum_ledger_time"
      )
      val getParties: client.Summary = summary(Prefix :+ "get_parties")
      val listKnownParties: client.Summary = summary(Prefix :+ "list_known_parties")
      val listLfPackages: client.Summary = summary(Prefix :+ "list_lf_packages")
      val getLfArchive: client.Summary = summary(Prefix :+ "get_lf_archive")
      val prune: client.Summary = summary(Prefix :+ "prune")
      val getTransactionMetering: client.Summary = summary(Prefix :+ "get_transaction_metering")

      val publishTransaction: client.Summary = summary(Prefix :+ "publish_transaction")
      val publishPartyAllocation: client.Summary = summary(Prefix :+ "publish_party_allocation")
      val uploadPackages: client.Summary = summary(Prefix :+ "upload_packages")
      val publishConfiguration: client.Summary = summary(Prefix :+ "publish_configuration")

      val decodeTransactionLogUpdate: client.Summary =
        summary(Prefix :+ "transaction_log_update_decode")
      val transactionLogUpdatesBufferSize: client.Gauge =
        gauge(Prefix :+ "transaction_log_updates_buffer_size")

      val transactionTreesBufferSize: client.Gauge =
        gauge(Prefix :+ "transaction_trees_buffer_size")
      val flatTransactionsBufferSize: client.Gauge =
        gauge(Prefix :+ "flat_transactions_buffer_size")

      val contractStateEventsBufferSize: client.Gauge =
        gauge(Prefix :+ "contract_state_events_buffer_size")

      val acsRetrievalSequentialProcessing: client.Summary =
        summary(Prefix :+ "acs_retrieval_sequential_processing")

      // FIXME Name mushing and inconsistencies here, tracked by https://github.com/digital-asset/daml/issues/5926
      object db {
        private val Prefix: MetricName = index.Prefix :+ "db"

        val storePartyEntry: client.Histogram = histogram(Prefix :+ "store_party_entry")
        val storePackageEntry: client.Histogram = histogram(Prefix :+ "store_package_entry")

        val storeTransaction: client.Histogram = histogram(Prefix :+ "store_ledger_entry")
        val storeTransactionCombined: client.Summary =
          summary(Prefix :+ "store_ledger_entry_combined")
        val storeTransactionEvents: client.Histogram = histogram(
          Prefix :+ "store_ledger_entry_events"
        )
        val storeTransactionState: client.Histogram = histogram(
          Prefix :+ "store_ledger_entry_state"
        )
        val storeTransactionCompletion: client.Summary =
          summary(Prefix :+ "store_ledger_entry_completion")

        val storeRejection: client.Histogram = histogram(Prefix :+ "store_rejection")
        val storeConfigurationEntry: client.Histogram = histogram(
          Prefix :+ "store_configuration_entry"
        )

        val lookupLedgerId: client.Histogram = histogram(Prefix :+ "lookup_ledger_id")
        val lookupParticipantId: client.Histogram = histogram(Prefix :+ "lookup_participant_id")
        val lookupLedgerEnd: client.Histogram = histogram(Prefix :+ "lookup_ledger_end")
        val lookupLedgerEndSequentialId: client.Summary =
          summary(Prefix :+ "lookup_ledger_end_sequential_id")
        val lookupTransaction: client.Histogram = histogram(Prefix :+ "lookup_transaction")
        val lookupLedgerConfiguration: client.Summary =
          summary(Prefix :+ "lookup_ledger_configuration")
        val lookupKey: client.Histogram = histogram(Prefix :+ "lookup_key")
        val lookupActiveContract: client.Histogram = histogram(Prefix :+ "lookup_active_contract")
        val lookupMaximumLedgerTime: client.Histogram = histogram(
          Prefix :+ "lookup_maximum_ledger_time"
        )
        val getParties: client.Histogram = histogram(Prefix :+ "get_parties")
        val listKnownParties: client.Histogram = histogram(Prefix :+ "list_known_parties")
        val listLfPackages: client.Histogram = histogram(Prefix :+ "list_lf_packages")
        val getLfArchive: client.Histogram = histogram(Prefix :+ "get_lf_archive")
        val prune: client.Histogram = histogram(Prefix :+ "prune")

        private val createDbMetrics: String => DatabaseMetrics =
          new DatabaseMetrics(Prefix, _)

        private val overall = createDbMetrics("all")
        val waitAll: Summary = overall.waitTimer
        val execAll: Summary = overall.executionTimer

        val getCompletions: DatabaseMetrics = createDbMetrics("get_completions")
        val getLedgerId: DatabaseMetrics = createDbMetrics("get_ledger_id")
        val getParticipantId: DatabaseMetrics = createDbMetrics("get_participant_id")
        val getLedgerEnd: DatabaseMetrics = createDbMetrics("get_ledger_end")
        val getInitialLedgerEnd: DatabaseMetrics = createDbMetrics("get_initial_ledger_end")
        val initializeLedgerParameters: DatabaseMetrics = createDbMetrics(
          "initialize_ledger_parameters"
        )
        val initializeParticipantId: DatabaseMetrics = createDbMetrics("initialize_participant_id")
        val lookupConfiguration: DatabaseMetrics = createDbMetrics("lookup_configuration")
        val loadConfigurationEntries: DatabaseMetrics = createDbMetrics(
          "load_configuration_entries"
        )
        val storeConfigurationEntryDbMetrics: DatabaseMetrics = createDbMetrics(
          "store_configuration_entry"
        ) // FIXME Base name conflicts with storeConfigurationEntry
        val storePartyEntryDbMetrics: DatabaseMetrics = createDbMetrics(
          "store_party_entry"
        ) // FIXME Base name conflicts with storePartyEntry
        val loadPartyEntries: DatabaseMetrics = createDbMetrics("load_party_entries")
        val getTransactionLogUpdates: DatabaseMetrics = createDbMetrics(
          "get_transaction_log_updates"
        )

        object storeTransactionDbMetrics extends DatabaseMetrics(Prefix, "store_ledger_entry") {
          // outside of SQL transaction
          val prepareBatches: client.Histogram = histogram(dbPrefix :+ "prepare_batches")

          // in order within SQL transaction
          val eventsBatch: client.Histogram = histogram(dbPrefix :+ "events_batch")
          val deleteContractWitnessesBatch: client.Summary =
            summary(dbPrefix :+ "delete_contract_witnesses_batch")
          val deleteContractsBatch: client.Histogram = histogram(
            dbPrefix :+ "delete_contracts_batch"
          )
          val nullifyPastKeysBatch: client.Summary =
            summary(dbPrefix :+ "nullify_contract_keys_batch")
          val insertContractsBatch: client.Histogram = histogram(
            dbPrefix :+ "insert_contracts_batch"
          )
          val insertContractWitnessesBatch: client.Summary =
            summary(dbPrefix :+ "insert_contract_witnesses_batch")

          val insertCompletion: client.Histogram = histogram(dbPrefix :+ "insert_completion")
          val updateLedgerEnd: client.Histogram = histogram(dbPrefix :+ "update_ledger_end")
        }
        val storeRejectionDbMetrics: DatabaseMetrics = createDbMetrics(
          "store_rejection"
        ) // FIXME Base name conflicts with storeRejection
        val loadParties: DatabaseMetrics = createDbMetrics("load_parties")
        val loadAllParties: DatabaseMetrics = createDbMetrics("load_all_parties")
        val loadPackages: DatabaseMetrics = createDbMetrics("load_packages")
        val loadArchive: DatabaseMetrics = createDbMetrics("load_archive")
        val storePackageEntryDbMetrics: DatabaseMetrics = createDbMetrics(
          "store_package_entry"
        ) // FIXME Base name conflicts with storePackageEntry
        val loadPackageEntries: DatabaseMetrics = createDbMetrics("load_package_entries")
        val pruneDbMetrics: DatabaseMetrics = createDbMetrics(
          "prune"
        ) // FIXME Base name conflicts with prune
        val truncateAllTables: DatabaseMetrics = createDbMetrics("truncate_all_tables")
        val lookupActiveContractDbMetrics: DatabaseMetrics = createDbMetrics(
          "lookup_active_contract"
        ) // FIXME Base name conflicts with lookupActiveContract
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
        val getContractStateEvents: DatabaseMetrics = createDbMetrics(
          "get_contract_state_events"
        )
        val loadStringInterningEntries: DatabaseMetrics = createDbMetrics(
          "loadStringInterningEntries"
        )

        val meteringAggregator: DatabaseMetrics = createDbMetrics("metering_aggregator")
        val initializeMeteringAggregator: DatabaseMetrics = createDbMetrics(
          "initialize_metering_aggregator"
        )

        object translation {
          private val Prefix: MetricName = db.Prefix :+ "translation"
          val cache = CacheMetrics(Prefix :+ "cache")
          val getLfPackage: client.Summary = summary(Prefix :+ "get_lf_package")
        }

        object compression {
          private val Prefix: MetricName = db.Prefix :+ "compression"

          val createArgumentCompressed: client.Summary =
            summary(Prefix :+ "create_argument_compressed")
          val createArgumentUncompressed: client.Summary =
            summary(Prefix :+ "create_argument_uncompressed")
          val createKeyValueCompressed: client.Summary =
            summary(Prefix :+ "create_key_value_compressed")
          val createKeyValueUncompressed: client.Summary =
            summary(Prefix :+ "create_key_value_uncompressed")
          val exerciseArgumentCompressed: client.Summary =
            summary(Prefix :+ "exercise_argument_compressed")
          val exerciseArgumentUncompressed: client.Summary =
            summary(Prefix :+ "exercise_argument_uncompressed")
          val exerciseResultCompressed: client.Summary =
            summary(Prefix :+ "exercise_result_compressed")
          val exerciseResultUncompressed: client.Summary =
            summary(Prefix :+ "exercise_result_uncompressed")

        }

      }
    }

    object indexer {
      private val Prefix: MetricName = daml.Prefix :+ "indexer"

      // TODO Prometheus metrics: implement
      val lastReceivedRecordTime = new VarGauge[Long](0)
//      registry.register(Prefix :+ "last_received_record_time", lastReceivedRecordTime)

      val lastReceivedOffset = new VarGauge[String]("<none>")
//      registry.register(Prefix :+ "last_received_offset", lastReceivedOffset)

      // TODO Prometheus metrics: implement this
//      registerGauge(
//        Prefix :+ "current_record_time_lag",
//        () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.getValue,
//        registry,
//      )

      val stateUpdateProcessing: client.Summary = summary(Prefix :+ "processed_state_updates")

      // TODO Prometheus metrics: implement
      val ledgerEndSequentialId = new VarGauge[Long](0L)
//      registry.register(Prefix :+ "ledger_end_sequential_id", ledgerEndSequentialId)
    }

    // TODO append-only: streamline metrics upon cleanup
    object parallelIndexer {
      private val Prefix: MetricName = daml.Prefix :+ "parallel_indexer"

      val initialization = new DatabaseMetrics(Prefix, "initialization")

      // Number of state updates persisted to the database
      // (after the effect of the corresponding Update is persisted into the database,
      // and before this effect is visible via moving the ledger end forward)
      val updates: client.Gauge = gauge(Prefix :+ "updates")

      // The size of the queue before the indexer
      val inputBufferLength: client.Gauge = gauge(Prefix :+ "input_buffer_length")

      // Input mapping stage
      // Translating state updates to data objects corresponding to individual SQL insert statements
      object inputMapping {
        private val Prefix: MetricName = parallelIndexer.Prefix :+ "inputmapping"

        // Bundle of metrics coming from instrumentation of the underlying thread-pool
        val executor: MetricName = Prefix :+ "executor"

        // The batch size, i.e., the number of state updates per database submission
        val batchSize: client.Summary = summary(Prefix :+ "batch_size")
      }

      // Batching stage
      // Translating batch data objects to db-specific DTO batches
      object batching {
        private val Prefix: MetricName = parallelIndexer.Prefix :+ "batching"

        // Bundle of metrics coming from instrumentation of the underlying thread-pool
        val executor: MetricName = Prefix :+ "executor"
      }

      // Sequence Mapping stage
      object seqMapping {
        private val Prefix: MetricName = parallelIndexer.Prefix :+ "seqmapping"

        // The latency, which during an update element is residing in the seq-mapping-stage.
        val duration: client.Summary = summary(Prefix :+ "duration")
      }

      // Ingestion stage
      // Parallel ingestion of prepared data into the database
      val ingestion = new DatabaseMetrics(Prefix, "ingestion")

      // Tail ingestion stage
      // The throttled update of ledger end parameters
      val tailIngestion = new DatabaseMetrics(Prefix, "tail_ingestion")
    }

    object services {
      private val Prefix: MetricName = daml.Prefix :+ "services"

      object index {
        private val Prefix: MetricName = services.Prefix :+ "index"

        val listLfPackages: client.Summary = summary(Prefix :+ "list_lf_packages")
        val getLfArchive: client.Summary = summary(Prefix :+ "get_lf_archive")
        val packageEntries: client.Summary = summary(Prefix :+ "package_entries")
        val getLedgerConfiguration: client.Summary = summary(Prefix :+ "get_ledger_configuration")
        val currentLedgerEnd: client.Summary = summary(Prefix :+ "current_ledger_end")
        val getCompletions: client.Summary = summary(Prefix :+ "get_completions")
        val getCompletionsLimited: client.Summary = summary(Prefix :+ "get_completions_limited")
        val transactions: client.Summary = summary(Prefix :+ "transactions")
        val transactionTrees: client.Summary = summary(Prefix :+ "transaction_trees")
        val getTransactionById: client.Summary = summary(Prefix :+ "get_transaction_by_id")
        val getTransactionTreeById: client.Summary = summary(Prefix :+ "get_transaction_tree_by_id")
        val getActiveContracts: client.Summary = summary(Prefix :+ "get_active_contracts")
        val lookupActiveContract: client.Summary = summary(Prefix :+ "lookup_active_contract")
        val lookupContractKey: client.Summary = summary(Prefix :+ "lookup_contract_key")
        val lookupMaximumLedgerTime: client.Summary = summary(
          Prefix :+ "lookup_maximum_ledger_time"
        )
        val getLedgerId: client.Summary = summary(Prefix :+ "get_ledger_id")
        val getParticipantId: client.Summary = summary(Prefix :+ "get_participant_id")
        val getParties: client.Summary = summary(Prefix :+ "get_parties")
        val listKnownParties: client.Summary = summary(Prefix :+ "list_known_parties")
        val partyEntries: client.Summary = summary(Prefix :+ "party_entries")
        val lookupConfiguration: client.Summary = summary(Prefix :+ "lookup_configuration")
        val configurationEntries: client.Summary = summary(Prefix :+ "configuration_entries")
        val prune: client.Summary = summary(Prefix :+ "prune")
        val getTransactionMetering: client.Summary = summary(Prefix :+ "get_transaction_metering")

        object streamsBuffer {
          private val Prefix: MetricName = index.Prefix :+ "streams_buffer"

          def push(qualifier: String): client.Summary = summary(Prefix :+ qualifier :+ "push")
          def slice(qualifier: String): client.Summary = summary(Prefix :+ qualifier :+ "slice")
          def prune(qualifier: String): client.Summary = summary(Prefix :+ qualifier :+ "prune")

          val transactionTreesTotal: client.Gauge =
            gauge(Prefix :+ "transaction_trees_total")
          val transactionTreesBuffered: client.Gauge =
            gauge(Prefix :+ "transaction_trees_buffered")

          val flatTransactionsTotal: client.Gauge =
            gauge(Prefix :+ "flat_transactions_total")
          val flatTransactionsBuffered: client.Gauge =
            gauge(Prefix :+ "flat_transactions_buffered")

          val getTransactionTrees: client.Summary =
            summary(Prefix :+ "get_transaction_trees")
          val getFlatTransactions: client.Summary =
            summary(Prefix :+ "get_flat_transactions")

          val toFlatTransactions: client.Summary = summary(Prefix :+ "to_flat_transactions")
          val toTransactionTrees: client.Summary = summary(Prefix :+ "to_transaction_trees")

          val transactionTreesBufferSize: client.Gauge =
            gauge(Prefix :+ "transaction_trees_buffer_size")
          val flatTransactionsBufferSize: client.Gauge =
            gauge(Prefix :+ "flat_transactions_buffer_size")
        }

        val getContractStateEventsChunkSize: client.Summary =
          summary(Prefix :+ "get_contract_state_events_chunk_fetch_size")
        val getTransactionLogUpdatesChunkSize: client.Summary =
          summary(Prefix :+ "get_transaction_log_updates_chunk_fetch_size")
      }

      object read {
        private val Prefix: MetricName = services.Prefix :+ "read"

        val getLedgerInitialConditions: client.Summary =
          summary(Prefix :+ "get_ledger_initial_conditions")
        val stateUpdates: client.Summary = summary(Prefix :+ "state_updates")
      }

      object write {
        private val Prefix: MetricName = services.Prefix :+ "write"

        val submitTransaction: client.Summary = summary(Prefix :+ "submit_transaction")
        val submitTransactionRunning: client.Gauge = gauge(Prefix :+ "submit_transaction_running")
        val uploadPackages: client.Summary = summary(Prefix :+ "upload_packages")
        val allocateParty: client.Summary = summary(Prefix :+ "allocate_party")
        val submitConfiguration: client.Summary = summary(Prefix :+ "submit_configuration")
        val prune: client.Summary = summary(Prefix :+ "prune")
      }
    }

    object HttpJsonApi {
      private val Prefix: MetricName = daml.Prefix :+ "http_json_api"

      object Db {
        private val Prefix: MetricName = HttpJsonApi.Prefix :+ "db"
        val fetchByIdFetch: client.Summary = summary(Prefix :+ "fetch_by_id_fetch")
        val fetchByIdQuery: client.Summary = summary(Prefix :+ "fetch_by_id_query")
        val fetchByKeyFetch: client.Summary = summary(Prefix :+ "fetch_by_key_fetch")
        val fetchByKeyQuery: client.Summary = summary(Prefix :+ "fetch_by_key_query")
        val searchFetch: client.Summary = summary(Prefix :+ "search_fetch")
        val searchQuery: client.Summary = summary(Prefix :+ "search_query")
      }

      val surrogateTemplateIdCache = CacheMetrics(Prefix :+ "surrogate_tpid_cache")

      // Meters how long processing of a command submission request takes
      val commandSubmissionTimer: client.Summary = summary(Prefix :+ "command_submission_timing")
      // Meters how long processing of a query GET request takes
      val queryAllTimer: client.Summary = summary(Prefix :+ "query_all_timing")
      // Meters how long processing of a query POST request takes
      val queryMatchingTimer: client.Summary = summary(Prefix :+ "query_matching_timing")
      // Meters how long processing of a fetch request takes
      val fetchTimer: client.Summary = summary(Prefix :+ "fetch_timing")
      // Meters how long processing of a get party/parties request takes
      val getPartyTimer: client.Summary = summary(Prefix :+ "get_party_timing")
      // Meters how long processing of a party management request takes
      val allocatePartyTimer: client.Summary = summary(Prefix :+ "allocate_party_timing")
      // Meters how long processing of a package download request takes
      val downloadPackageTimer: client.Summary = summary(Prefix :+ "download_package_timing")
      // Meters how long processing of a package upload request takes
      val uploadPackageTimer: client.Summary = summary(Prefix :+ "upload_package_timing")
      // Meters how long parsing and decoding of an incoming json payload takes
      val incomingJsonParsingAndValidationTimer: client.Summary =
        summary(Prefix :+ "incoming_json_parsing_and_validation_timing")
      // Meters how long the construction of the response json payload takes
      val responseCreationTimer: client.Summary = summary(Prefix :+ "response_creation_timing")
      // Meters how long a find by contract key database operation takes
      val dbFindByContractKey: client.Summary = summary(Prefix :+ "db_find_by_contract_key_timing")
      // Meters how long a find by contract id database operation takes
      val dbFindByContractId: client.Summary = summary(Prefix :+ "db_find_by_contract_id_timing")
      // Meters how long processing of the command submission request takes on the ledger
      val commandSubmissionLedgerTimer: client.Summary =
        summary(Prefix :+ "command_submission_ledger_timing")
      // Meters http requests throughput
      val httpRequestThroughput: client.Counter = counter(Prefix :+ "http_request_throughput")
      // Meters how many websocket connections are currently active
      val websocketRequestCounter: client.Gauge = gauge(Prefix :+ "websocket_request_count")
      // Meters command submissions throughput
      val commandSubmissionThroughput: client.Counter =
        counter(Prefix :+ "command_submission_throughput")
      // Meters package uploads throughput
      val uploadPackagesThroughput: client.Counter = counter(Prefix :+ "upload_packages_throughput")
      // Meters party allocation throughput
      val allocatePartyThroughput: client.Counter = counter(Prefix :+ "allocation_party_throughput")
    }
  }
}
