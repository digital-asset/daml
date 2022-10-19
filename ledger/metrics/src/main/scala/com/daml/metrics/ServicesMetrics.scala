// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.{Counter, Histogram, Meter, Timer}

import com.codahale.metrics.MetricRegistry

class ServicesMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {

  object index extends MetricHandle.Factory {
    override val prefix: MetricName = ServicesMetrics.this.prefix :+ "index"
    override val registry: MetricRegistry = ServicesMetrics.this.registry

    @MetricDoc.Tag(
      summary = "The time to execute an index service operation.",
      description = """The index service is an internal component responsible for access to the
                      |index db data. Its operations are invoked whenever a client request received
                      |over the ledger api requires access to the index db. This metric captures
                      |time statistics of such operations.""",
      qualification = Debug,
    )
    val operationForDocs: Timer = Timer(prefix :+ "<operation>", null)

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

      @MetricDoc.Tag(
        summary = "The time to add a new event into the buffer.",
        description = """The in-memory fan-out buffer is a buffer that stores the last ingested
                        |maxBufferSize accepted and rejected submission updates as
                        |TransactionLogUpdate. It allows bypassing IndexDB persistence fetches for
                        |recent updates for flat and transaction tree streams, command completion
                        |streams and by-event-id and by-transaction-id flat and transaction tree
                        |lookups. This metric exposes the time spent on adding a new event into the
                        |buffer.""",
        qualification = Debug,
      )
      val push: Timer = timer(prefix :+ "push")

      @MetricDoc.Tag(
        summary = "The time to remove all elements from the in-memory fan-out buffer.",
        description = """It is possible to remove the oldest entries of the in-memory fan out
                        |buffer. This metric exposes the time needed to prune the buffer.""",
        qualification = Debug,
      )
      val prune: Timer = timer(prefix :+ "prune")

      @MetricDoc.Tag(
        summary = "The size of the in-memory fan-out buffer.",
        description = """The actual size of the in-memory fan-out buffer. This metric is mostly
                        |targeted for debugging purposes.""",
        qualification = Debug,
      )
      val bufferSize: Histogram = histogram(prefix :+ "size")
    }

    case class BufferedReader(streamName: String) extends MetricHandle.Factory {
      override val prefix: MetricName = index.prefix :+ s"${streamName}_buffer_reader"
      override val registry: MetricRegistry = index.registry

      @MetricDoc.Tag(
        summary =
          "The total number of the events fetched (either from the buffer or the persistence).",
        description = """The buffer reader serves processed and filtered events from the buffer,
                        |with fallback to persistence fetches if the bounds are not within the
                        |buffer's range bounds. This metric exposes the total number of the fetched
                        |events.""",
        qualification = Debug,
      )
      val fetchedTotal: Counter = counter(prefix :+ "fetched_total")

      @MetricDoc.Tag(
        summary = "The total number of the events fetched from the buffer.",
        description = """The buffer reader serves processed and filtered events from the buffer,
                        |with fallback to persistence fetches if the bounds are not within the
                        |buffer's range bounds. This metric counts the number of events delivered
                        |exclusively from the buffer.""",
        qualification = Debug,
      )
      val fetchedBuffered: Counter = counter(prefix :+ "fetched_buffered")

      @MetricDoc.Tag(
        summary = "The time needed to fetch an event (either from the buffer or the persistence).",
        description = """The buffer reader serves stream events in chunks downstream to the gRPC
                        |layer and to the client. This metric times the duration needed for serving
                        |a chunk of events. This metric has been created with debugging purposes
                        |in mind.""",
        qualification = Debug,
      )
      val fetchTimer: Timer = timer(prefix :+ "fetch")

      @MetricDoc.Tag(
        summary = "The time to convert a buffered fetched event to a ledger api stream response.",
        description = """Entries are stored in the buffer in a custom deserialized representation.
                        |When served to the gRPC layer, the entries are processed and serialized.
                        |This metric times this operation.""",
        qualification = Debug,
      )
      val conversion: Timer = timer(prefix :+ "conversion")

      @MetricDoc.Tag(
        summary = "The time to fetch a chunk of events from the buffer",
        description = """The events are served from the buffer in chunks, respecting the input
                        |bounds and a predicate filter. This metric times this operation.""",
        qualification = Debug,
      )
      val slice: Timer = timer(prefix :+ "slice")

      @MetricDoc.Tag(
        summary = "The size of the slice requested.",
        description = """The events are served from the buffer in chunks. This metric gauges the
                        |chunk size delivered downstream.""",
        qualification = Debug,
      )
      val sliceSize: Histogram = histogram(prefix :+ "slice_size")
    }
  }

  object read extends MetricHandle.Factory {
    override val prefix: MetricName = ServicesMetrics.this.prefix :+ "read"
    override val registry: MetricRegistry = index.registry

    @MetricDoc.Tag(
      summary = "The time to execute a read service operation.",
      description = """The read service is an internal interface for reading the events from the
                      |synchronization interfaces. The metrics expose the time needed to execute
                      |each operation.""",
      qualification = Debug,
    )
    val readOperationForDocs: Timer = Timer(prefix :+ "<operation>", null)

    val getLedgerInitialConditions: Timer = timer(prefix :+ "get_ledger_initial_conditions")
    val stateUpdates: Timer = timer(prefix :+ "state_updates")
  }

  object write extends MetricHandle.Factory {
    override val prefix: MetricName = ServicesMetrics.this.prefix :+ "write"
    override val registry: MetricRegistry = index.registry

    @MetricDoc.Tag(
      summary = "The time to execute a write service operation.",
      description = """The write service is an internal interface for changing the state through
                      |the synchronization services. The methods in this interface are all methods
                      |that are supported uniformly across all ledger implementations. This metric
                      |exposes the time needed to execute each operation.""",
      qualification = Debug,
    )
    val writeOperationForDocs: Timer = Timer(prefix :+ "<operation>", null)

    val submitTransaction: Timer = timer(prefix :+ "submit_transaction")
    val submitTransactionRunning: Meter = meter(prefix :+ "submit_transaction_running")
    val uploadPackages: Timer = timer(prefix :+ "upload_packages")
    val allocateParty: Timer = timer(prefix :+ "allocate_party")
    val submitConfiguration: Timer = timer(prefix :+ "submit_configuration")
    val prune: Timer = timer(prefix :+ "prune")
  }
}
