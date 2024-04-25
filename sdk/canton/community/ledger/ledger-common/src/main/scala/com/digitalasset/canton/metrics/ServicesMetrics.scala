// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class ServicesMetrics(
    prefix: MetricName,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  object index {
    private val prefix = ServicesMetrics.this.prefix :+ "index"
    private val baseInfo = MetricInfo(
      prefix,
      summary = "The time to execute an index service operation.",
      description = """The index service is an internal component responsible for access to the
                      |index db data. Its operations are invoked whenever a client request received
                      |over the ledger api requires access to the index db. This metric captures
                      |time statistics of such operations.""",
      qualification = MetricQualification.Debug,
    )

    val listLfPackages: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("list_lf_packages"))

    val getLfArchive: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("get_lf_archive"))

    val packageEntries: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("package_entries"))

    val getLedgerConfiguration: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_ledger_configuration"))

    val currentLedgerEnd: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("current_ledger_end"))

    val latestPrunedOffsets: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("latest_pruned_offsets"))

    val getCompletions: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_completions"))

    val getCompletionsLimited: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_completions_limited"))

    val transactions: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("transactions"))

    val transactionTrees: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("transaction_trees"))

    val getTransactionById: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_transaction_by_id"))

    val getTransactionTreeById: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_transaction_tree_by_id"))

    val getActiveContracts: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_active_contracts"))

    val lookupActiveContract: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("lookup_active_contract"))

    val lookupContractState: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("lookup_contract_state"))

    val lookupContractKey: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("lookup_contract_key"))

    val getEventsByContractId: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_events_by_contract_id"))

    val getEventsByContractKey: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_events_by_contract_key"))

    val lookupMaximumLedgerTime: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("lookup_maximum_ledger_time"))

    val getParticipantId: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_participant_id"))

    val getParties: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("get_parties"))

    val listKnownParties: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("list_known_parties"))

    val partyEntries: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("party_entries"))

    val lookupConfiguration: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("lookup_configuration"))

    val configurationEntries: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("configuration_entries"))

    val prune: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("prune"))

    val getTransactionMetering: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_transaction_metering"))

    object InMemoryFanoutBuffer {
      val prefix: MetricName = index.prefix :+ "in_memory_fan_out_buffer"

      val push: Timer = openTelemetryMetricsFactory.timer(
        MetricInfo(
          prefix :+ "push",
          summary = "The time to add a new event into the buffer.",
          description = """The in-memory fan-out buffer is a buffer that stores the last ingested
                        |maxBufferSize accepted and rejected submission updates as
                        |TransactionLogUpdate. It allows bypassing IndexDB persistence fetches for
                        |recent updates for flat and transaction tree streams, command completion
                        |streams and by-event-id and by-transaction-id flat and transaction tree
                        |lookups. This metric exposes the time spent on adding a new event into the
                        |buffer.""",
          qualification = MetricQualification.Debug,
        )
      )

      val prune: Timer = openTelemetryMetricsFactory.timer(
        MetricInfo(
          prefix :+ "prune",
          summary = "The time to remove all elements from the in-memory fan-out buffer.",
          description = """It is possible to remove the oldest entries of the in-memory fan out
                        |buffer. This metric exposes the time needed to prune the buffer.""",
          qualification = MetricQualification.Debug,
        )
      )

      val bufferSize: Histogram = openTelemetryMetricsFactory.histogram(
        MetricInfo(
          prefix :+ "size",
          summary = "The size of the in-memory fan-out buffer.",
          description = """The actual size of the in-memory fan-out buffer. This metric is mostly
                        |targeted for debugging purposes.""",
          qualification = MetricQualification.Saturation,
        )
      )
    }

    case class BufferedReader(streamName: String) {
      val prefix: MetricName = index.prefix :+ s"${streamName}_buffer_reader"

      val fetchedTotal: Counter = openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "fetched_total",
          summary =
            "The total number of the events fetched (either from the buffer or the persistence).",
          description = """The buffer reader serves processed and filtered events from the buffer,
                        |with fallback to persistence fetches if the bounds are not within the
                        |buffer's range bounds. This metric exposes the total number of the fetched
                        |events.""",
          qualification = MetricQualification.Debug,
        )
      )

      val fetchedBuffered: Counter =
        openTelemetryMetricsFactory.counter(
          MetricInfo(
            prefix :+ "fetched_buffered",
            summary = "The total number of the events fetched from the buffer.",
            description = """The buffer reader serves processed and filtered events from the buffer,
                          |with fallback to persistence fetches if the bounds are not within the
                          |buffer's range bounds. This metric counts the number of events delivered
                          |exclusively from the buffer.""",
            qualification = MetricQualification.Debug,
          )
        )

      val fetchTimer: Timer = openTelemetryMetricsFactory.timer(
        MetricInfo(
          prefix :+ "fetch",
          summary =
            "The time needed to fetch an event (either from the buffer or the persistence).",
          description = """The buffer reader serves stream events in chunks downstream to the gRPC
                        |layer and to the client. This metric times the duration needed for serving
                        |a chunk of events. This metric has been created with debugging purposes
                        |in mind.""",
          qualification = MetricQualification.Debug,
        )
      )

      val conversion: Timer = openTelemetryMetricsFactory.timer(
        MetricInfo(
          prefix :+ "conversion",
          summary = "The time to convert a buffered fetched event to a ledger api stream response.",
          description = """Entries are stored in the buffer in a custom deserialized representation.
                        |When served to the gRPC layer, the entries are processed and serialized.
                        |This metric times this operation.""",
          qualification = MetricQualification.Debug,
        )
      )

      val slice: Timer = openTelemetryMetricsFactory.timer(
        MetricInfo(
          prefix :+ "slice",
          summary = "The time to fetch a chunk of events from the buffer",
          description = """The events are served from the buffer in chunks, respecting the input
                        |bounds and a predicate filter. This metric times this operation.""",
          qualification = MetricQualification.Debug,
        )
      )

      val sliceSize: Histogram = openTelemetryMetricsFactory.histogram(
        MetricInfo(
          prefix :+ "slice_size",
          summary = "The size of the slice requested.",
          description = """The events are served from the buffer in chunks. This metric gauges the
                        |chunk size delivered downstream.""",
          qualification = MetricQualification.Debug,
        )
      )
    }
  }

  object read {
    val prefix: MetricName = ServicesMetrics.this.prefix :+ "read"
    private val baseInfo = MetricInfo(
      prefix,
      summary = "The time to execute a read service operation.",
      description = """The read service is an internal interface for reading the events from the
                      |synchronization interfaces. The metrics expose the time needed to execute
                      |each operation.""",
      qualification = MetricQualification.Debug,
    )

    val stateUpdates: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("state_updates"))

    val getConnectedDomains: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("get_connected_domains"))

    val incompleteReassignmentOffsets: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("incomplete_reassignment_offsets"))
  }

  object write {

    val prefix: MetricName = ServicesMetrics.this.prefix :+ "write"
    private val baseInfo = MetricInfo(
      prefix,
      summary = "The time to execute a write service operation.",
      description = """The write service is an internal interface for changing the state through
                      |the synchronization services. The methods in this interface are all methods
                      |that are supported uniformly across all ledger implementations. This metric
                      |exposes the time needed to execute each operation.""",
      qualification = MetricQualification.Debug,
    )

    val submitTransaction: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("submit_transaction"))

    val submitTransactionRunning: Counter =
      openTelemetryMetricsFactory.counter(baseInfo.extend("submit_transaction_running"))

    val submitReassignment: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("submit_reassignment"))

    val submitReassignmentRunning: Counter =
      openTelemetryMetricsFactory.counter(baseInfo.extend("submit_reassignment_running"))

    val uploadPackages: Timer =
      openTelemetryMetricsFactory.timer(baseInfo.extend("upload_packages"))

    val allocateParty: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("allocate_party"))

    val prune: Timer = openTelemetryMetricsFactory.timer(baseInfo.extend("prune"))
  }

  object pruning extends PruningMetrics(prefix :+ "pruning", openTelemetryMetricsFactory)
}
