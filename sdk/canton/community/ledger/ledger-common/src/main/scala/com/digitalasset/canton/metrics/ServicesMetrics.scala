// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.MetricQualification.{Debug, Saturation}
import com.daml.metrics.api.{MetricDoc, MetricName}

class ServicesMetrics(
    prefix: MetricName,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  @MetricDoc.FanTag(
    representative = "daml.services.index.<operation>",
    summary = "The time to execute an index service operation.",
    description = """The index service is an internal component responsible for access to the
                    |index db data. Its operations are invoked whenever a client request received
                    |over the ledger api requires access to the index db. This metric captures
                    |time statistics of such operations.""",
    qualification = Debug,
  )
  object index {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "index"

    @MetricDoc.FanInstanceTag
    val listLfPackages: Timer = openTelemetryMetricsFactory.timer(prefix :+ "list_lf_packages")
    @MetricDoc.FanInstanceTag
    val getLfArchive: Timer = openTelemetryMetricsFactory.timer(prefix :+ "get_lf_archive")
    @MetricDoc.FanInstanceTag
    val packageEntries: Timer = openTelemetryMetricsFactory.timer(prefix :+ "package_entries")
    @MetricDoc.FanInstanceTag
    val getLedgerConfiguration: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_ledger_configuration")
    @MetricDoc.FanInstanceTag
    val currentLedgerEnd: Timer = openTelemetryMetricsFactory.timer(prefix :+ "current_ledger_end")
    @MetricDoc.FanInstanceTag
    val latestPrunedOffsets: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "latest_pruned_offsets")
    @MetricDoc.FanInstanceTag
    val getCompletions: Timer = openTelemetryMetricsFactory.timer(prefix :+ "get_completions")
    @MetricDoc.FanInstanceTag
    val getCompletionsLimited: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_completions_limited")
    @MetricDoc.FanInstanceTag
    val transactions: Timer = openTelemetryMetricsFactory.timer(prefix :+ "transactions")
    @MetricDoc.FanInstanceTag
    val transactionTrees: Timer = openTelemetryMetricsFactory.timer(prefix :+ "transaction_trees")
    @MetricDoc.FanInstanceTag
    val getTransactionById: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_transaction_by_id")
    @MetricDoc.FanInstanceTag
    val getTransactionTreeById: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_transaction_tree_by_id")
    @MetricDoc.FanInstanceTag
    val getActiveContracts: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_active_contracts")
    @MetricDoc.FanInstanceTag
    val lookupActiveContract: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "lookup_active_contract")
    @MetricDoc.FanInstanceTag
    val lookupContractState: Timer = openTelemetryMetricsFactory.timer(
      prefix :+ "lookup_contract_state"
    )
    @MetricDoc.FanInstanceTag
    val lookupContractKey: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "lookup_contract_key")
    @MetricDoc.FanInstanceTag
    val getEventsByContractId: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_events_by_contract_id")
    @MetricDoc.FanInstanceTag
    val getEventsByContractKey: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_events_by_contract_key")
    @MetricDoc.FanInstanceTag
    val lookupMaximumLedgerTime: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "lookup_maximum_ledger_time")
    @MetricDoc.FanInstanceTag
    val getParticipantId: Timer = openTelemetryMetricsFactory.timer(prefix :+ "get_participant_id")
    @MetricDoc.FanInstanceTag
    val getParties: Timer = openTelemetryMetricsFactory.timer(prefix :+ "get_parties")
    @MetricDoc.FanInstanceTag
    val listKnownParties: Timer = openTelemetryMetricsFactory.timer(prefix :+ "list_known_parties")
    @MetricDoc.FanInstanceTag
    val partyEntries: Timer = openTelemetryMetricsFactory.timer(prefix :+ "party_entries")
    @MetricDoc.FanInstanceTag
    val lookupConfiguration: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "lookup_configuration")
    @MetricDoc.FanInstanceTag
    val configurationEntries: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "configuration_entries")
    @MetricDoc.FanInstanceTag
    val prune: Timer = openTelemetryMetricsFactory.timer(prefix :+ "prune")
    @MetricDoc.FanInstanceTag
    val getTransactionMetering: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_transaction_metering")

    object InMemoryFanoutBuffer {
      val prefix: MetricName = index.prefix :+ "in_memory_fan_out_buffer"

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
      val push: Timer = openTelemetryMetricsFactory.timer(prefix :+ "push")

      @MetricDoc.Tag(
        summary = "The time to remove all elements from the in-memory fan-out buffer.",
        description = """It is possible to remove the oldest entries of the in-memory fan out
                        |buffer. This metric exposes the time needed to prune the buffer.""",
        qualification = Debug,
      )
      val prune: Timer = openTelemetryMetricsFactory.timer(prefix :+ "prune")

      @MetricDoc.Tag(
        summary = "The size of the in-memory fan-out buffer.",
        description = """The actual size of the in-memory fan-out buffer. This metric is mostly
                        |targeted for debugging purposes.""",
        qualification = Saturation,
      )
      val bufferSize: Histogram = openTelemetryMetricsFactory.histogram(prefix :+ "size")
    }

    case class BufferedReader(streamName: String) {
      val prefix: MetricName = index.prefix :+ s"${streamName}_buffer_reader"

      @MetricDoc.Tag(
        summary =
          "The total number of the events fetched (either from the buffer or the persistence).",
        description = """The buffer reader serves processed and filtered events from the buffer,
                        |with fallback to persistence fetches if the bounds are not within the
                        |buffer's range bounds. This metric exposes the total number of the fetched
                        |events.""",
        qualification = Debug,
      )
      val fetchedTotal: Counter = openTelemetryMetricsFactory.counter(prefix :+ "fetched_total")

      @MetricDoc.Tag(
        summary = "The total number of the events fetched from the buffer.",
        description = """The buffer reader serves processed and filtered events from the buffer,
                        |with fallback to persistence fetches if the bounds are not within the
                        |buffer's range bounds. This metric counts the number of events delivered
                        |exclusively from the buffer.""",
        qualification = Debug,
      )
      val fetchedBuffered: Counter =
        openTelemetryMetricsFactory.counter(prefix :+ "fetched_buffered")

      @MetricDoc.Tag(
        summary = "The time needed to fetch an event (either from the buffer or the persistence).",
        description = """The buffer reader serves stream events in chunks downstream to the gRPC
                        |layer and to the client. This metric times the duration needed for serving
                        |a chunk of events. This metric has been created with debugging purposes
                        |in mind.""",
        qualification = Debug,
      )
      val fetchTimer: Timer = openTelemetryMetricsFactory.timer(prefix :+ "fetch")

      @MetricDoc.Tag(
        summary = "The time to convert a buffered fetched event to a ledger api stream response.",
        description = """Entries are stored in the buffer in a custom deserialized representation.
                        |When served to the gRPC layer, the entries are processed and serialized.
                        |This metric times this operation.""",
        qualification = Debug,
      )
      val conversion: Timer = openTelemetryMetricsFactory.timer(prefix :+ "conversion")

      @MetricDoc.Tag(
        summary = "The time to fetch a chunk of events from the buffer",
        description = """The events are served from the buffer in chunks, respecting the input
                        |bounds and a predicate filter. This metric times this operation.""",
        qualification = Debug,
      )
      val slice: Timer = openTelemetryMetricsFactory.timer(prefix :+ "slice")

      @MetricDoc.Tag(
        summary = "The size of the slice requested.",
        description = """The events are served from the buffer in chunks. This metric gauges the
                        |chunk size delivered downstream.""",
        qualification = Debug,
      )
      val sliceSize: Histogram = openTelemetryMetricsFactory.histogram(prefix :+ "slice_size")
    }
  }

  @MetricDoc.FanTag(
    representative = "daml.services.read.<operation>",
    summary = "The time to execute a read service operation.",
    description = """The read service is an internal interface for reading the events from the
                    |synchronization interfaces. The metrics expose the time needed to execute
                    |each operation.""",
    qualification = Debug,
  )
  object read {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "read"

    @MetricDoc.FanInstanceTag
    val stateUpdates: Timer = openTelemetryMetricsFactory.timer(prefix :+ "state_updates")
    @MetricDoc.FanInstanceTag
    val getConnectedDomains: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "get_connected_domains")
    @MetricDoc.FanInstanceTag
    val incompleteReassignmentOffsets: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "incomplete_reassignment_offsets")
  }

  @MetricDoc.FanTag(
    representative = "daml.services.write.<operation>",
    summary = "The time to execute a write service operation.",
    description = """The write service is an internal interface for changing the state through
                    |the synchronization services. The methods in this interface are all methods
                    |that are supported uniformly across all ledger implementations. This metric
                    |exposes the time needed to execute each operation.""",
    qualification = Debug,
  )
  object write {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "write"

    @MetricDoc.FanInstanceTag
    val submitTransaction: Timer = openTelemetryMetricsFactory.timer(prefix :+ "submit_transaction")
    @MetricDoc.FanInstanceTag
    val submitTransactionRunning: Counter =
      openTelemetryMetricsFactory.counter(prefix :+ "submit_transaction_running")
    @MetricDoc.FanInstanceTag
    val submitReassignment: Timer =
      openTelemetryMetricsFactory.timer(prefix :+ "submit_reassignment")
    @MetricDoc.FanInstanceTag
    val submitReassignmentRunning: Counter =
      openTelemetryMetricsFactory.counter(prefix :+ "submit_reassignment_running")
    @MetricDoc.FanInstanceTag
    val uploadPackages: Timer = openTelemetryMetricsFactory.timer(prefix :+ "upload_packages")
    @MetricDoc.FanInstanceTag
    val allocateParty: Timer = openTelemetryMetricsFactory.timer(prefix :+ "allocate_party")
    @MetricDoc.FanInstanceTag
    val prune: Timer = openTelemetryMetricsFactory.timer(prefix :+ "prune")
  }

  object pruning extends PruningMetrics(prefix :+ "pruning", openTelemetryMetricsFactory)
}
