// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.metrics.HistogramInventory.Item

class ServicesHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val indexPrefix: MetricName = prefix :+ "index"
  private[metrics] val fanoutPrefix: MetricName = indexPrefix :+ "in_memory_fan_out_buffer"

  private[metrics] val fanoutBufferSize: Item =
    Item(
      fanoutPrefix :+ "size",
      summary = "The size of the in-memory fan-out buffer.",
      description = """The actual size of the in-memory fan-out buffer. This metric is mostly
                      |targeted for debugging purposes.""",
      qualification = MetricQualification.Saturation,
    )
  private[metrics] val fanoutPush: Item =
    Item(
      fanoutPrefix :+ "push",
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

  private[metrics] val fanoutPruning: Item =
    Item(
      fanoutPrefix :+ "prune",
      summary = "The time to remove all elements from the in-memory fan-out buffer.",
      description = """It is possible to remove the oldest entries of the in-memory fan out
                        |buffer. This metric exposes the time needed to prune the buffer.""",
      qualification = MetricQualification.Debug,
    )

  private val baseInfo = MetricInfo(
    prefix,
    summary = "The time to execute an index service operation.",
    description = """The index service is an internal component responsible for access to the
                    |index db data. Its operations are invoked whenever a client request received
                    |over the ledger api requires access to the index db. This metric captures
                    |time statistics of such operations.""",
    qualification = MetricQualification.Debug,
  )
  private def extend(name: String, template: MetricInfo): Item = {
    val info = template.extend(name)
    val item = new Item(
      info.name,
      info.summary,
      info.qualification,
      info.description,
      info.labelsWithDescription,
    )
    inventory.register(item)
    item
  }

  private[metrics] val listLfPackages: Item = extend("list_lf_packages", baseInfo)
  private[metrics] val getLfArchive: Item = extend("get_lf_archive", baseInfo)
  private[metrics] val packageEntries: Item = extend("package_entries", baseInfo)
  private[metrics] val currentLedgerEnd: Item = extend("current_ledger_end", baseInfo)
  private[metrics] val latestPrunedOffsets: Item = extend("latest_pruned_offsets", baseInfo)
  private[metrics] val getCompletions: Item = extend("get_completions", baseInfo)
  private[metrics] val transactions: Item = extend("transactions", baseInfo)
  private[metrics] val transactionTrees: Item = extend("transaction_trees", baseInfo)
  private[metrics] val getTransactionById: Item = extend("get_transaction_by_id", baseInfo)
  private[metrics] val getTransactionTreeById: Item = extend("get_transaction_tree_by_id", baseInfo)
  private[metrics] val getActiveContracts: Item = extend("get_active_contracts", baseInfo)
  private[metrics] val lookupActiveContract: Item = extend("lookup_active_contract", baseInfo)
  private[metrics] val lookupContractState: Item = extend("lookup_contract_state", baseInfo)
  private[metrics] val lookupContractKey: Item = extend("lookup_contract_key", baseInfo)
  private[metrics] val getEventsByContractId: Item = extend("get_events_by_contract_id", baseInfo)
  private[metrics] val lookupMaximumLedgerTime: Item =
    extend("lookup_maximum_ledger_time", baseInfo)
  private[metrics] val getParticipantId: Item = extend("get_participant_id", baseInfo)
  private[metrics] val getParties: Item = extend("get_parties", baseInfo)
  private[metrics] val listKnownParties: Item = extend("list_known_parties", baseInfo)
  private[metrics] val partyEntries: Item = extend("party_entries", baseInfo)
  private[metrics] val lookupConfiguration: Item = extend("lookup_configuration", baseInfo)
  private[metrics] val prune: Item = extend("prune", baseInfo)
  private[metrics] val getTransactionMetering: Item = extend("get_transaction_metering", baseInfo)

  private[metrics] val bufferedReaderPrefix: MetricName = indexPrefix :+ "buffer_reader"

  private[metrics] val bufferedReaderFetchTimer: Item = Item(
    bufferedReaderPrefix :+ "fetch",
    summary = "The time needed to fetch an event (either from the buffer or the persistence).",
    description = """The buffer reader serves stream events in chunks downstream to the gRPC
                      |layer and to the client. This metric times the duration needed for serving
                      |a chunk of events. This metric has been created with debugging purposes
                      |in mind.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val bufferedReaderConversion: Item = Item(
    bufferedReaderPrefix :+ "conversion",
    summary = "The time to convert a buffered fetched event to a ledger api stream response.",
    description = """Entries are stored in the buffer in a custom deserialized representation.
                      |When served to the gRPC layer, the entries are processed and serialized.
                      |This metric times this operation.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val bufferedReaderSlice: Item = Item(
    bufferedReaderPrefix :+ "slice",
    summary = "The time to fetch a chunk of events from the buffer",
    description = """The events are served from the buffer in chunks, respecting the input
                      |bounds and a predicate filter. This metric times this operation.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val bufferedReaderSliceSize: Item = Item(
    bufferedReaderPrefix :+ "slice_size",
    summary = "The size of the slice requested.",
    description = """The events are served from the buffer in chunks. This metric gauges the
                      |chunk size delivered downstream.""",
    qualification = MetricQualification.Debug,
  )

  private val readBaseInfo = MetricInfo(
    prefix :+ "read",
    summary = "The time to execute a read service operation.",
    description = """The read service is an internal interface for reading the events from the
                    |synchronization interfaces. The metrics expose the time needed to execute
                    |each operation.""",
    qualification = MetricQualification.Debug,
  )
  private[metrics] val readStateUpdates: Item = extend("state_updates", readBaseInfo)

  private[metrics] val readGetConnectedDomains: Item = extend("get_connected_domains", readBaseInfo)

  private[metrics] val readIncompleteReassignmentOffsets: Item =
    extend("incomplete_reassignment_offsets", readBaseInfo)

  private[metrics] val readListLfPackages: Item = extend("list_lf_packages", readBaseInfo)
  private[metrics] val readGetLfArchive: Item = extend("get_lf_archive", readBaseInfo)
  private[metrics] val readValidateDar: Item = extend("validate_dar", readBaseInfo)

  private[metrics] val writeBaseInfo = MetricInfo(
    indexPrefix :+ "write",
    summary = "The time to execute a write service operation.",
    description = """The write service is an internal interface for changing the state through
                    |the synchronization services. The methods in this interface are all methods
                    |that are supported uniformly across all ledger implementations. This metric
                    |exposes the time needed to execute each operation.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val writeSubmitTransaction: Item = extend("submit_transaction", writeBaseInfo)

  private[metrics] val writeSubmitReassignment: Item = extend("submit_reassignment", writeBaseInfo)

  private[metrics] val writeUploadPackages: Item = extend("upload_packages", writeBaseInfo)

  private[metrics] val writeAllocateParty: Item = extend("allocate_party", writeBaseInfo)

  private[metrics] val writePrune: Item = extend("prune", writeBaseInfo)

}
class ServicesMetrics(
    inventory: ServicesHistograms,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  private val prefix = inventory.prefix
  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  object index {
    private val prefix = inventory.indexPrefix

    val listLfPackages: Timer = openTelemetryMetricsFactory.timer(inventory.listLfPackages.info)
    val getLfArchive: Timer = openTelemetryMetricsFactory.timer(inventory.getLfArchive.info)
    val packageEntries: Timer = openTelemetryMetricsFactory.timer(inventory.packageEntries.info)
    val currentLedgerEnd: Timer = openTelemetryMetricsFactory.timer(inventory.currentLedgerEnd.info)
    val latestPrunedOffsets: Timer =
      openTelemetryMetricsFactory.timer(inventory.latestPrunedOffsets.info)
    val getCompletions: Timer = openTelemetryMetricsFactory.timer(inventory.getCompletions.info)
    val transactions: Timer = openTelemetryMetricsFactory.timer(inventory.transactions.info)
    val transactionTrees: Timer = openTelemetryMetricsFactory.timer(inventory.transactionTrees.info)
    val getTransactionById: Timer =
      openTelemetryMetricsFactory.timer(inventory.getTransactionById.info)
    val getTransactionTreeById: Timer =
      openTelemetryMetricsFactory.timer(inventory.getTransactionTreeById.info)
    val getActiveContracts: Timer =
      openTelemetryMetricsFactory.timer(inventory.getActiveContracts.info)
    val lookupActiveContract: Timer =
      openTelemetryMetricsFactory.timer(inventory.lookupActiveContract.info)

    val lookupContractState: Timer =
      openTelemetryMetricsFactory.timer(inventory.lookupContractState.info)

    val lookupContractKey: Timer =
      openTelemetryMetricsFactory.timer(inventory.lookupContractKey.info)

    val getEventsByContractId: Timer =
      openTelemetryMetricsFactory.timer(inventory.getEventsByContractId.info)

    val lookupMaximumLedgerTime: Timer =
      openTelemetryMetricsFactory.timer(inventory.lookupMaximumLedgerTime.info)

    val getParticipantId: Timer =
      openTelemetryMetricsFactory.timer(inventory.getParticipantId.info)

    val getParties: Timer =
      openTelemetryMetricsFactory.timer(inventory.getParties.info)

    val listKnownParties: Timer =
      openTelemetryMetricsFactory.timer(inventory.listKnownParties.info)

    val partyEntries: Timer =
      openTelemetryMetricsFactory.timer(inventory.partyEntries.info)

    val lookupConfiguration: Timer =
      openTelemetryMetricsFactory.timer(inventory.lookupConfiguration.info)

    val prune: Timer = openTelemetryMetricsFactory.timer(inventory.prune.info)

    val getTransactionMetering: Timer =
      openTelemetryMetricsFactory.timer(inventory.getTransactionMetering.info)

    object InMemoryFanoutBuffer {
      val prefix: MetricName = inventory.fanoutPrefix

      val push: Timer = openTelemetryMetricsFactory.timer(
        inventory.fanoutPush.info
      )

      val prune: Timer = openTelemetryMetricsFactory.timer(
        inventory.fanoutPruning.info
      )

      val bufferSize: Histogram = openTelemetryMetricsFactory.histogram(
        inventory.fanoutBufferSize.info
      )
    }

    case class BufferedReader(streamName: String) {
      implicit val metricsContext: MetricsContext = MetricsContext("stream" -> streamName)
      val prefix: MetricName = index.prefix :+ "buffer_reader"

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

      val fetchTimer: Timer =
        openTelemetryMetricsFactory.timer(inventory.bufferedReaderFetchTimer.info)

      val conversion: Timer =
        openTelemetryMetricsFactory.timer(inventory.bufferedReaderConversion.info)

      val slice: Timer = openTelemetryMetricsFactory.timer(inventory.bufferedReaderSlice.info)

      val sliceSize: Histogram =
        openTelemetryMetricsFactory.histogram(inventory.bufferedReaderSliceSize.info)
    }
  }

  object read {

    val stateUpdates: Timer = openTelemetryMetricsFactory.timer(inventory.readStateUpdates.info)

    val getConnectedDomains: Timer =
      openTelemetryMetricsFactory.timer(inventory.readGetConnectedDomains.info)

    val incompleteReassignmentOffsets: Timer =
      openTelemetryMetricsFactory.timer(inventory.readIncompleteReassignmentOffsets.info)

    val listLfPackages: Timer = openTelemetryMetricsFactory.timer(inventory.readListLfPackages.info)
    val getLfArchive: Timer = openTelemetryMetricsFactory.timer(inventory.readGetLfArchive.info)
    val validateDar: Timer = openTelemetryMetricsFactory.timer(inventory.readValidateDar.info)
  }

  object write {

    val submitTransaction: Timer =
      openTelemetryMetricsFactory.timer(inventory.writeSubmitTransaction.info)

    val submitTransactionRunning: Counter =
      openTelemetryMetricsFactory.counter(
        inventory.writeBaseInfo.extend("submit_transaction_running")
      )

    val submitReassignment: Timer =
      openTelemetryMetricsFactory.timer(inventory.writeSubmitReassignment.info)

    val submitReassignmentRunning: Counter =
      openTelemetryMetricsFactory.counter(
        inventory.writeBaseInfo.extend("submit_reassignment_running")
      )

    val uploadPackages: Timer =
      openTelemetryMetricsFactory.timer(inventory.writeUploadPackages.info)

    val allocateParty: Timer = openTelemetryMetricsFactory.timer(inventory.writeAllocateParty.info)

    val prune: Timer = openTelemetryMetricsFactory.timer(inventory.writePrune.info)
  }

  object pruning extends PruningMetrics(prefix :+ "pruning", openTelemetryMetricsFactory)
}
