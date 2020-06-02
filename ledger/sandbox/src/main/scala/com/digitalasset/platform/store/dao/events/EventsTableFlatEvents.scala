// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.EventsTableQueries.format

private[events] trait EventsTableFlatEvents { this: EventsTable =>

  private val createdFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createArgument = createArgument,
            createSignatories = createSignatories,
            createObservers = createObservers,
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            eventWitnesses = eventWitnesses,
          )
        )
    }

  private val archivedFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Archived]] =
    archivedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Archived(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            eventWitnesses = eventWitnesses,
          )
        )
    }

  val rawFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent]] =
    createdFlatEventParser | archivedFlatEventParser

  private val selectColumns =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "ledger_effective_time",
      "workflow_id",
      "participant_events.event_id",
      "contract_id",
      "template_id",
      "create_argument",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
    ).mkString(", ")

  private val orderByColumns =
    Seq("event_offset", "transaction_id", "node_index").mkString(", ")

  private val groupByColumns =
    Seq(
      "event_offset",
      "transaction_id",
      "ledger_effective_time",
      "command_id",
      "workflow_id",
      "participant_events.event_id",
      "contract_id",
      "template_id",
      "create_argument",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
    ).mkString(", ")

  def prepareLookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] =
    route(requestingParties)(
      single = singlePartyLookup(transactionId, _),
      multi = multiPartyLookup(transactionId, _),
    )

  private def singlePartyLookup(
      transactionId: TransactionId,
      requestingParty: Party,
  ): SimpleSql[Row] =
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and flat_event_witnesses && array[$requestingParty]::varchar[] order by #$orderByColumns"

  private def multiPartyLookup(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val partiesStr = format(requestingParties)
    SQL"select #$selectColumns, flat_event_witnesses as event_witnesses, case when submitter in (#$partiesStr) then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and flat_event_witnesses && array[#$partiesStr]::varchar[] group by (#$groupByColumns) order by #$orderByColumns"
  }

  private val getFlatTransactionsQueries =
    new EventsTableFlatEventsRangeQueries.GetTransactions(
      selectColumns = selectColumns,
      groupByColumns = groupByColumns,
      orderByColumns = orderByColumns,
    )

  def preparePagedGetFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row] =
    getFlatTransactionsQueries(
      (startExclusive, endInclusive),
      filter,
      pageSize,
      previousEventNodeIndex,
    )

  private val getActiveContractsQueries =
    new EventsTableFlatEventsRangeQueries.GetActiveContracts(
      selectColumns = selectColumns,
      groupByColumns = groupByColumns,
      orderByColumns = orderByColumns,
    )

  def preparePagedGetActiveContracts(
      lastOffsetFromPrevPage: Offset,
      activeAt: Offset,
      filter: FilterRelation,
      pageSize: Int,
      lastEventNodeIndexFromPrevPage: Option[Int]
  ): SimpleSql[Row] =
    getActiveContractsQueries(
      (lastOffsetFromPrevPage, activeAt),
      filter,
      pageSize,
      lastEventNodeIndexFromPrevPage,
    )
}
