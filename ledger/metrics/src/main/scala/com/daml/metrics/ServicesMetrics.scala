// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Saturation, Traffic}
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Timer}
import com.daml.metrics.api.dropwizard.{DropwizardFactory, DropwizardTimer}
import com.daml.metrics.api.{MetricDoc, MetricName}

class ServicesMetrics(val prefix: MetricName, override val registry: MetricRegistry)
    extends DropwizardFactory {

  @MetricDoc.GroupRepresenterTag(
    representative = "daml.services.index.<operation>",
    summary = "The time to execute an index service operation.",
    description = """The index service is an internal component responsible for access to the
                    |index db data. Its operations are invoked whenever a client request received
                    |over the ledger api requires access to the index db. This metric captures
                    |time statistics of such operations.""",
    qualification = Debug,
  )
  object index extends DropwizardFactory {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "index"
    override val registry: MetricRegistry = ServicesMetrics.this.registry

    @MetricDoc.GroupByNameTag
    val listLfPackages: Timer = timer(prefix :+ "list_lf_packages")
    @MetricDoc.GroupByNameTag
    val getLfArchive: Timer = timer(prefix :+ "get_lf_archive")
    @MetricDoc.GroupByNameTag
    val packageEntries: Timer = timer(prefix :+ "package_entries")
    @MetricDoc.GroupByNameTag
    val getLedgerConfiguration: Timer = timer(prefix :+ "get_ledger_configuration")
    @MetricDoc.GroupByNameTag
    val currentLedgerEnd: Timer = timer(prefix :+ "current_ledger_end")
    @MetricDoc.GroupByNameTag
    val getCompletions: Timer = timer(prefix :+ "get_completions")
    @MetricDoc.GroupByNameTag
    val getCompletionsLimited: Timer = timer(prefix :+ "get_completions_limited")
    @MetricDoc.GroupByNameTag
    val transactions: Timer = timer(prefix :+ "transactions")
    @MetricDoc.GroupByNameTag
    val transactionTrees: Timer = timer(prefix :+ "transaction_trees")
    @MetricDoc.GroupByNameTag
    val getTransactionById: Timer = timer(prefix :+ "get_transaction_by_id")
    @MetricDoc.GroupByNameTag
    val getTransactionTreeById: Timer = timer(prefix :+ "get_transaction_tree_by_id")
    @MetricDoc.GroupByNameTag
    val getActiveContracts: Timer = timer(prefix :+ "get_active_contracts")
    @MetricDoc.GroupByNameTag
    val lookupActiveContract: Timer = timer(prefix :+ "lookup_active_contract")
    @MetricDoc.GroupByNameTag
    val lookupContractStateWithoutDivulgence: Timer = timer(
      prefix :+ "lookup_contract_state_without_divulgence"
    )
    @MetricDoc.GroupByNameTag
    val lookupContractKey: Timer = timer(prefix :+ "lookup_contract_key")
    @MetricDoc.GroupByNameTag
    val lookupMaximumLedgerTime: Timer = timer(prefix :+ "lookup_maximum_ledger_time")
    @MetricDoc.GroupByNameTag
    val getParticipantId: Timer = timer(prefix :+ "get_participant_id")
    @MetricDoc.GroupByNameTag
    val getParties: Timer = timer(prefix :+ "get_parties")
    @MetricDoc.GroupByNameTag
    val listKnownParties: Timer = timer(prefix :+ "list_known_parties")
    @MetricDoc.GroupByNameTag
    val partyEntries: Timer = timer(prefix :+ "party_entries")
    @MetricDoc.GroupByNameTag
    val lookupConfiguration: Timer = timer(prefix :+ "lookup_configuration")
    @MetricDoc.GroupByNameTag
    val configurationEntries: Timer = timer(prefix :+ "configuration_entries")
    @MetricDoc.GroupByNameTag
    val prune: Timer = timer(prefix :+ "prune")
    @MetricDoc.GroupByNameTag
    val getTransactionMetering: Timer = timer(prefix :+ "get_transaction_metering")

    object InMemoryFanoutBuffer extends DropwizardFactory {
      val prefix: MetricName = index.prefix :+ "in_memory_fan_out_buffer"
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
        qualification = Saturation,
      )
      val bufferSize: Histogram = histogram(prefix :+ "size")
    }

    case class BufferedReader(streamName: String) extends DropwizardFactory {
      val prefix: MetricName = index.prefix :+ s"${streamName}_buffer_reader"
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

  @MetricDoc.GroupRepresenterTag(
    representative = "daml.services.read.<operation>",
    summary = "The time to execute a read service operation.",
    description = """The read service is an internal interface for reading the events from the
                    |synchronization interfaces. The metrics expose the time needed to execute
                    |each operation.""",
    qualification = Debug,
  )
  object read extends DropwizardFactory {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "read"
    override val registry: MetricRegistry = index.registry

    @MetricDoc.GroupByNameTag
    val getLedgerInitialConditions: Timer = timer(prefix :+ "get_ledger_initial_conditions")
    @MetricDoc.GroupByNameTag
    val stateUpdates: Timer = timer(prefix :+ "state_updates")
  }

  @MetricDoc.GroupRepresenterTag(
    representative = "daml.services.write.<operation>",
    summary = "The time to execute a write service operation.",
    description = """The write service is an internal interface for changing the state through
                    |the synchronization services. The methods in this interface are all methods
                    |that are supported uniformly across all ledger implementations. This metric
                    |exposes the time needed to execute each operation.""",
    qualification = Debug,
  )
  object write extends DropwizardFactory {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "write"
    override val registry: MetricRegistry = index.registry

    @MetricDoc.Tag(
      summary = "The number of submitted transactions by the write service.",
      description = """The write service is an internal interface for changing the state through
                      |the synchronization services. The methods in this interface are all methods
                      |that are supported uniformly across all ledger implementations. This metric
                      |exposes the total number of the sumbitted transactions.""",
      qualification = Traffic,
    )
    val submitOperationForDocs: Timer =
      DropwizardTimer(prefix :+ "submit_transaction" :+ "count", null)

    @MetricDoc.GroupByNameTag
    val submitTransaction: Timer = timer(prefix :+ "submit_transaction")
    @MetricDoc.GroupByNameTag
    val submitTransactionRunning: Counter = counter(prefix :+ "submit_transaction_running")
    @MetricDoc.GroupByNameTag
    val uploadPackages: Timer = timer(prefix :+ "upload_packages")
    @MetricDoc.GroupByNameTag
    val allocateParty: Timer = timer(prefix :+ "allocate_party")
    @MetricDoc.GroupByNameTag
    val submitConfiguration: Timer = timer(prefix :+ "submit_configuration")
    @MetricDoc.GroupByNameTag
    val prune: Timer = timer(prefix :+ "prune")
  }
}
