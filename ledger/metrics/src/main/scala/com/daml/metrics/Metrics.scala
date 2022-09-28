// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._

import java.time.Instant

object Metrics {
  def fromSharedMetricRegistries(registryName: String): Metrics =
    new Metrics(SharedMetricRegistries.getOrCreate(registryName))
}

final class Metrics(val registry: MetricRegistry) {

  def register(name: MetricName, gaugeSupplier: MetricSupplier[Gauge[_]]): Unit =
    registerGauge(name, gaugeSupplier, registry)

  object test {
    private val Prefix: MetricName = MetricName("test")

    val db: DatabaseMetrics = new DatabaseMetrics(Prefix, "db", registry)
  }

  object daml {
    private val Prefix: MetricName = MetricName.Daml

    object commands {
      private val Prefix: MetricName = daml.Prefix :+ "commands"

      val validation: Timer = registry.timer(Prefix :+ "validation")
      val submissions: Timer = registry.timer(Prefix :+ "submissions")
      val submissionsRunning: Meter = registry.meter(Prefix :+ "submissions_running")

      val failedCommandInterpretations: Meter =
        registry.meter(Prefix :+ "failed_command_interpretations")
      val delayedSubmissions: Meter =
        registry.meter(Prefix :+ "delayed_submissions")
      val validSubmissions: Meter =
        registry.meter(Prefix :+ "valid_submissions")

      val inputBufferLength: Counter = registry.counter(Prefix :+ "input_buffer_length")
      val inputBufferCapacity: Counter = registry.counter(Prefix :+ "input_buffer_capacity")
      val inputBufferDelay: Timer = registry.timer(Prefix :+ "input_buffer_delay")
      val maxInFlightLength: Counter = registry.counter(Prefix :+ "max_in_flight_length")
      val maxInFlightCapacity: Counter = registry.counter(Prefix :+ "max_in_flight_capacity")
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

      // Total time for command execution (including data fetching)
      val total: Timer = registry.timer(Prefix :+ "total")
      val totalRunning: Meter = registry.meter(Prefix :+ "total_running")

      // Commands being executed by the engine (not currently fetching data)
      val engine: Timer = registry.timer(Prefix :+ "engine")
      val engineRunning: Meter = registry.meter(Prefix :+ "engine_running")

      object cache {
        private val Prefix: MetricName = execution.Prefix :+ "cache"

        val keyState: CacheMetrics = new CacheMetrics(Prefix :+ "key_state", registry)
        val contractState: CacheMetrics =
          new CacheMetrics(Prefix :+ "contract_state", registry)

        val registerCacheUpdate: Timer = registry.timer(Prefix :+ "register_update")

        val resolveDivulgenceLookup: Counter =
          registry.counter(Prefix :+ "resolve_divulgence_lookup")

        val resolveFullLookup: Counter =
          registry.counter(Prefix :+ "resolve_full_lookup")

        val readThroughNotFound: Counter = registry.counter(Prefix :+ "read_through_not_found")
      }
    }

    object lapi {
      private val Prefix: MetricName = daml.Prefix :+ "lapi"

      def forMethod(name: String): Timer = registry.timer(Prefix :+ name)

      object return_status {
        private val Prefix: MetricName = lapi.Prefix :+ "return_status"
        def forCode(code: String): Counter = registry.counter(Prefix :+ code)
      }

      object threadpool {
        private val Prefix: MetricName = lapi.Prefix :+ "threadpool"

        val apiServices: MetricName = Prefix :+ "api-services"

        val inMemoryFanOut: MetricName = Prefix :+ "in_memory_fan_out"

        object indexBypass {
          private val Prefix: MetricName = threadpool.Prefix :+ "index_bypass"
          val prepareUpdates: MetricName = Prefix :+ "prepare_updates"
          val updateInMemoryState: MetricName = Prefix :+ "update_in_memory_state"
        }
      }

      object streams {
        private val Prefix: MetricName = lapi.Prefix :+ "streams"

        val transactionTrees: Counter = registry.counter(Prefix :+ "transaction_trees_sent")
        val transactions: Counter = registry.counter(Prefix :+ "transactions_sent")
        val completions: Counter = registry.counter(Prefix :+ "completions_sent")
        val acs: Counter = registry.counter(Prefix :+ "acs_sent")

        val activeName: MetricName = Prefix :+ "active"
        val active: Counter = registry.counter(activeName)

      }
    }

    object userManagement {
      private val Prefix = daml.Prefix :+ "user_management"

      val cache = new CacheMetrics(Prefix :+ "cache", registry)

      private def createDbMetrics(name: String): DatabaseMetrics =
        new DatabaseMetrics(Prefix, name, registry)
      val getUserInfo: DatabaseMetrics = createDbMetrics("get_user_info")
      val createUser: DatabaseMetrics = createDbMetrics("create_user")
      val deleteUser: DatabaseMetrics = createDbMetrics("delete_user")
      val updateUser: DatabaseMetrics = createDbMetrics("update_user")
      val grantRights: DatabaseMetrics = createDbMetrics("grant_rights")
      val revokeRights: DatabaseMetrics = createDbMetrics("revoke_rights")
      val listUsers: DatabaseMetrics = createDbMetrics("list_users")
    }

    object partyRecordStore {
      private val Prefix = daml.Prefix :+ "party_record_store"

      private def createDbMetrics(name: String): DatabaseMetrics =
        new DatabaseMetrics(Prefix, name, registry)
      val getPartyRecord: DatabaseMetrics = createDbMetrics("get_party_record")
      val createPartyRecord: DatabaseMetrics = createDbMetrics("create_party_record")
      val updatePartyRecord: DatabaseMetrics = createDbMetrics("update_party_record")
    }

    object index {
      private val Prefix = daml.Prefix :+ "index"

      val transactionTreesBufferSize: Counter =
        registry.counter(Prefix :+ "transaction_trees_buffer_size")
      val flatTransactionsBufferSize: Counter =
        registry.counter(Prefix :+ "flat_transactions_buffer_size")
      val activeContractsBufferSize: Counter =
        registry.counter(Prefix :+ "active_contracts_buffer_size")
      val completionsBufferSize: Counter =
        registry.counter(Prefix :+ "completions_buffer_size")

      // FIXME Name mushing and inconsistencies here, tracked by https://github.com/digital-asset/daml/issues/5926
      object db {
        private val Prefix: MetricName = index.Prefix :+ "db"

        val storePartyEntry: Timer = registry.timer(Prefix :+ "store_party_entry")
        val storePackageEntry: Timer = registry.timer(Prefix :+ "store_package_entry")

        val storeTransactionCombined: Timer =
          registry.timer(Prefix :+ "store_ledger_entry_combined")

        val storeRejection: Timer = registry.timer(Prefix :+ "store_rejection")
        val storeConfigurationEntry: Timer = registry.timer(Prefix :+ "store_configuration_entry")

        val lookupLedgerId: Timer = registry.timer(Prefix :+ "lookup_ledger_id")
        val lookupParticipantId: Timer = registry.timer(Prefix :+ "lookup_participant_id")
        val lookupLedgerEnd: Timer = registry.timer(Prefix :+ "lookup_ledger_end")
        val lookupLedgerConfiguration: Timer =
          registry.timer(Prefix :+ "lookup_ledger_configuration")
        val lookupKey: Timer = registry.timer(Prefix :+ "lookup_key")
        val lookupActiveContract: Timer = registry.timer(Prefix :+ "lookup_active_contract")
        val getParties: Timer = registry.timer(Prefix :+ "get_parties")
        val listKnownParties: Timer = registry.timer(Prefix :+ "list_known_parties")
        val listLfPackages: Timer = registry.timer(Prefix :+ "list_lf_packages")
        val getLfArchive: Timer = registry.timer(Prefix :+ "get_lf_archive")
        val prune: Timer = registry.timer(Prefix :+ "prune")

        private val createDbMetrics: String => DatabaseMetrics =
          new DatabaseMetrics(Prefix, _, registry)

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
        ) // FIXME Base name conflicts with storeConfigurationEntry
        val storePartyEntryDbMetrics: DatabaseMetrics = createDbMetrics(
          "store_party_entry"
        ) // FIXME Base name conflicts with storePartyEntry
        val loadPartyEntries: DatabaseMetrics = createDbMetrics("load_party_entries")

        object storeTransactionDbMetrics
            extends DatabaseMetrics(Prefix, "store_ledger_entry", registry)

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
        val loadStringInterningEntries: DatabaseMetrics = createDbMetrics(
          "load_string_interning_entries"
        )

        val meteringAggregator: DatabaseMetrics = createDbMetrics("metering_aggregator")
        val initializeMeteringAggregator: DatabaseMetrics = createDbMetrics(
          "initialize_metering_aggregator"
        )

        object translation {
          private val Prefix: MetricName = db.Prefix :+ "translation"
          val getLfPackage: Timer = registry.timer(Prefix :+ "get_lf_package")
        }

        object compression {
          private val Prefix: MetricName = db.Prefix :+ "compression"

          val createArgumentCompressed: Histogram =
            registry.histogram(Prefix :+ "create_argument_compressed")
          val createArgumentUncompressed: Histogram =
            registry.histogram(Prefix :+ "create_argument_uncompressed")
          val createKeyValueCompressed: Histogram =
            registry.histogram(Prefix :+ "create_key_value_compressed")
          val createKeyValueUncompressed: Histogram =
            registry.histogram(Prefix :+ "create_key_value_uncompressed")
          val exerciseArgumentCompressed: Histogram =
            registry.histogram(Prefix :+ "exercise_argument_compressed")
          val exerciseArgumentUncompressed: Histogram =
            registry.histogram(Prefix :+ "exercise_argument_uncompressed")
          val exerciseResultCompressed: Histogram =
            registry.histogram(Prefix :+ "exercise_result_compressed")
          val exerciseResultUncompressed: Histogram =
            registry.histogram(Prefix :+ "exercise_result_uncompressed")
        }

        object threadpool {
          private val Prefix: MetricName = db.Prefix :+ "threadpool"

          val connection: MetricName = Prefix :+ "connection"
        }

      }

      val ledgerEndSequentialId = new VarGauge[Long](0L)
      registry.register(Prefix :+ "ledger_end_sequential_id", ledgerEndSequentialId)

      object lfValue {
        private val Prefix = index.Prefix :+ "lf_value"

        val computeInterfaceView: Timer = registry.timer(Prefix :+ "compute_interface_view")
      }

      object packageMetadata {
        private val Prefix = index.Prefix :+ "package_metadata"

        val decodeArchive: Timer = registry.timer(Prefix :+ "decode_archive")

        val viewInitialisation: Timer = registry.timer(Prefix :+ "view_init")
      }
    }

    object indexer {
      private val Prefix: MetricName = daml.Prefix :+ "indexer"

      val lastReceivedRecordTime = new VarGauge[Long](0)
      registry.register(Prefix :+ "last_received_record_time", lastReceivedRecordTime)

      val lastReceivedOffset = new VarGauge[String]("<none>")
      registry.register(Prefix :+ "last_received_offset", lastReceivedOffset)

      registerGauge(
        Prefix :+ "current_record_time_lag",
        () => () => Instant.now().toEpochMilli - lastReceivedRecordTime.getValue,
        registry,
      )

      val ledgerEndSequentialId = new VarGauge[Long](0L)
      registry.register(Prefix :+ "ledger_end_sequential_id", ledgerEndSequentialId)
    }

    // TODO append-only: streamline metrics upon cleanup
    object parallelIndexer {
      private val Prefix: MetricName = daml.Prefix :+ "parallel_indexer"

      val initialization = new DatabaseMetrics(Prefix, "initialization", registry)

      // Number of state updates persisted to the database
      // (after the effect of the corresponding Update is persisted into the database,
      // and before this effect is visible via moving the ledger end forward)
      val updates: Counter = registry.counter(Prefix :+ "updates")

      // The size of the queue before the indexer
      val inputBufferLength: Counter = registry.counter(Prefix :+ "input_buffer_length")

      /** The size of the queue after the indexer and before the in-memory state updating flow.
        * As opposed to [[inputBufferLength]], this counter counts batches, which are dynamically-sized
        * (for batch size, see [[inputMapping.batchSize]]).
        */
      val outputBatchedBufferLength: Counter =
        registry.counter(Prefix :+ "output_batched_buffer_length")

      // Input mapping stage
      // Translating state updates to data objects corresponding to individual SQL insert statements
      object inputMapping {
        private val Prefix: MetricName = parallelIndexer.Prefix :+ "inputmapping"

        // Bundle of metrics coming from instrumentation of the underlying thread-pool
        val executor: MetricName = Prefix :+ "executor"

        // The batch size, i.e., the number of state updates per database submission
        val batchSize: Histogram = registry.histogram(Prefix :+ "batch_size")
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
        val duration: Timer = registry.timer(Prefix :+ "duration")
      }

      // Ingestion stage
      // Parallel ingestion of prepared data into the database
      val ingestion = new DatabaseMetrics(Prefix, "ingestion", registry)

      // Tail ingestion stage
      // The throttled update of ledger end parameters
      val tailIngestion = new DatabaseMetrics(Prefix, "tail_ingestion", registry)
    }

    object services {
      private val Prefix: MetricName = daml.Prefix :+ "services"

      object index {
        private val Prefix: MetricName = services.Prefix :+ "index"

        val listLfPackages: Timer = registry.timer(Prefix :+ "list_lf_packages")
        val getLfArchive: Timer = registry.timer(Prefix :+ "get_lf_archive")
        val packageEntries: Timer = registry.timer(Prefix :+ "package_entries")
        val getLedgerConfiguration: Timer = registry.timer(Prefix :+ "get_ledger_configuration")
        val currentLedgerEnd: Timer = registry.timer(Prefix :+ "current_ledger_end")
        val getCompletions: Timer = registry.timer(Prefix :+ "get_completions")
        val getCompletionsLimited: Timer = registry.timer(Prefix :+ "get_completions_limited")
        val transactions: Timer = registry.timer(Prefix :+ "transactions")
        val transactionTrees: Timer = registry.timer(Prefix :+ "transaction_trees")
        val getTransactionById: Timer = registry.timer(Prefix :+ "get_transaction_by_id")
        val getTransactionTreeById: Timer = registry.timer(Prefix :+ "get_transaction_tree_by_id")
        val getActiveContracts: Timer = registry.timer(Prefix :+ "get_active_contracts")
        val lookupActiveContract: Timer = registry.timer(Prefix :+ "lookup_active_contract")
        val lookupContractAfterInterpretation: Timer =
          registry.timer(Prefix :+ "lookup_contract_after_interpretation")
        val lookupContractKey: Timer = registry.timer(Prefix :+ "lookup_contract_key")
        val lookupMaximumLedgerTime: Timer = registry.timer(Prefix :+ "lookup_maximum_ledger_time")
        val getParticipantId: Timer = registry.timer(Prefix :+ "get_participant_id")
        val getParties: Timer = registry.timer(Prefix :+ "get_parties")
        val listKnownParties: Timer = registry.timer(Prefix :+ "list_known_parties")
        val partyEntries: Timer = registry.timer(Prefix :+ "party_entries")
        val lookupConfiguration: Timer = registry.timer(Prefix :+ "lookup_configuration")
        val configurationEntries: Timer = registry.timer(Prefix :+ "configuration_entries")
        val prune: Timer = registry.timer(Prefix :+ "prune")
        val getTransactionMetering: Timer = registry.timer(Prefix :+ "get_transaction_metering")

        object InMemoryFanoutBuffer {
          val Prefix: MetricName = index.Prefix :+ "in_memory_fan_out_buffer"

          val push: Timer = registry.timer(Prefix :+ "push")
          val prune: Timer = registry.timer(Prefix :+ "prune")

          val bufferSize: Histogram = registry.histogram(Prefix :+ "size")
        }

        case class BufferedReader(streamName: String) {
          private val Prefix: MetricName = index.Prefix :+ s"${streamName}_buffer_reader"

          val fetchedTotal: Counter = registry.counter(Prefix :+ "fetched_total")
          val fetchedBuffered: Counter = registry.counter(Prefix :+ "fetched_buffered")
          val fetchTimer: Timer = registry.timer(Prefix :+ "fetch")
          val conversion: Timer = registry.timer(Prefix :+ "conversion")
          val slice: Timer = registry.timer(Prefix :+ "slice")
          val sliceSize: Histogram = registry.histogram(Prefix :+ "slice_size")
        }
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
        val submitTransactionRunning: Meter = registry.meter(Prefix :+ "submit_transaction_running")
        val uploadPackages: Timer = registry.timer(Prefix :+ "upload_packages")
        val allocateParty: Timer = registry.timer(Prefix :+ "allocate_party")
        val submitConfiguration: Timer = registry.timer(Prefix :+ "submit_configuration")
        val prune: Timer = registry.timer(Prefix :+ "prune")
      }
    }

    object HttpJsonApi {
      private val Prefix: MetricName = daml.Prefix :+ "http_json_api"

      object Db {
        private val Prefix: MetricName = HttpJsonApi.Prefix :+ "db"
        val fetchByIdFetch: Timer = registry.timer(Prefix :+ "fetch_by_id_fetch")
        val fetchByIdQuery: Timer = registry.timer(Prefix :+ "fetch_by_id_query")
        val fetchByKeyFetch: Timer = registry.timer(Prefix :+ "fetch_by_key_fetch")
        val fetchByKeyQuery: Timer = registry.timer(Prefix :+ "fetch_by_key_query")
        val searchFetch: Timer = registry.timer(Prefix :+ "search_fetch")
        val searchQuery: Timer = registry.timer(Prefix :+ "search_query")
      }

      val surrogateTemplateIdCache = new CacheMetrics(Prefix :+ "surrogate_tpid_cache", registry)

      // Meters how long processing of a command submission request takes
      val commandSubmissionTimer: Timer = registry.timer(Prefix :+ "command_submission_timing")
      // Meters how long processing of a query GET request takes
      val queryAllTimer: Timer = registry.timer(Prefix :+ "query_all_timing")
      // Meters how long processing of a query POST request takes
      val queryMatchingTimer: Timer = registry.timer(Prefix :+ "query_matching_timing")
      // Meters how long processing of a fetch request takes
      val fetchTimer: Timer = registry.timer(Prefix :+ "fetch_timing")
      // Meters how long processing of a get party/parties request takes
      val getPartyTimer: Timer = registry.timer(Prefix :+ "get_party_timing")
      // Meters how long processing of a party management request takes
      val allocatePartyTimer: Timer = registry.timer(Prefix :+ "allocate_party_timing")
      // Meters how long processing of a package download request takes
      val downloadPackageTimer: Timer = registry.timer(Prefix :+ "download_package_timing")
      // Meters how long processing of a package upload request takes
      val uploadPackageTimer: Timer = registry.timer(Prefix :+ "upload_package_timing")
      // Meters how long parsing and decoding of an incoming json payload takes
      val incomingJsonParsingAndValidationTimer: Timer =
        registry.timer(Prefix :+ "incoming_json_parsing_and_validation_timing")
      // Meters how long the construction of the response json payload takes
      val responseCreationTimer: Timer = registry.timer(Prefix :+ "response_creation_timing")
      // Meters how long a find by contract key database operation takes
      val dbFindByContractKey: Timer = registry.timer(Prefix :+ "db_find_by_contract_key_timing")
      // Meters how long a find by contract id database operation takes
      val dbFindByContractId: Timer = registry.timer(Prefix :+ "db_find_by_contract_id_timing")
      // Meters how long processing of the command submission request takes on the ledger
      val commandSubmissionLedgerTimer: Timer =
        registry.timer(Prefix :+ "command_submission_ledger_timing")
      // Meters http requests throughput
      val httpRequestThroughput: Meter = registry.meter(Prefix :+ "http_request_throughput")
      // Meters how many websocket connections are currently active
      val websocketRequestCounter: Counter = registry.counter(Prefix :+ "websocket_request_count")
      // Meters command submissions throughput
      val commandSubmissionThroughput: Meter =
        registry.meter(Prefix :+ "command_submission_throughput")
      // Meters package uploads throughput
      val uploadPackagesThroughput: Meter = registry.meter(Prefix :+ "upload_packages_throughput")
      // Meters party allocation throughput
      val allocatePartyThroughput: Meter = registry.meter(Prefix :+ "allocation_party_throughput")
    }
  }
}
