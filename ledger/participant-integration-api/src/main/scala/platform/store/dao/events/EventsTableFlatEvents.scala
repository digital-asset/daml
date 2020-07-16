// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._

private[events] trait EventsTableFlatEvents { this: EventsTable =>

  private val createdFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
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
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
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
      "event_sequential_id",
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

  def prepareLookupFlatTransactionById(sqlFunctions: SqlFunctions)(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] =
    route(requestingParties)(
      single = singlePartyLookup(sqlFunctions)(transactionId, _),
      multi = multiPartyLookup(sqlFunctions)(transactionId, _),
    )

  private def singlePartyLookup(sqlFunctions: SqlFunctions)(
      transactionId: TransactionId,
      requestingParty: Party,
  ): SimpleSql[Row] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", requestingParty)
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and #$witnessesWhereClause order by event_sequential_id"
  }

  private def multiPartyLookup(sqlFunctions: SqlFunctions)(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", requestingParties)
    SQL"select #$selectColumns, flat_event_witnesses as event_witnesses, case when submitter in ($requestingParties) then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and #$witnessesWhereClause group by (#$groupByColumns) order by event_sequential_id"
  }

  private def getFlatTransactionsQueries(sqlFunctions: SqlFunctions) =
    new EventsTableFlatEventsRangeQueries.GetTransactions(
      selectColumns = selectColumns,
      groupByColumns = groupByColumns,
      sqlFunctions = sqlFunctions,
    )

  def preparePagedGetFlatTransactions(sqlFunctions: SqlFunctions)(
      range: EventsRange[Long],
      filter: FilterRelation,
      pageSize: Int,
  ): SimpleSql[Row] =
    getFlatTransactionsQueries(sqlFunctions)(
      range,
      filter,
      pageSize,
    )

  private def getActiveContractsQueries(sqlFunctions: SqlFunctions) =
    new EventsTableFlatEventsRangeQueries.GetActiveContracts(
      selectColumns = selectColumns,
      groupByColumns = groupByColumns,
      sqlFunctions = sqlFunctions
    )

  def preparePagedGetActiveContracts(sqlFunctions: SqlFunctions)(
      range: EventsRange[(Offset, Long)],
      filter: FilterRelation,
      pageSize: Int
  ): SimpleSql[Row] =
    getActiveContractsQueries(sqlFunctions)(
      range,
      filter,
      pageSize
    )
}
