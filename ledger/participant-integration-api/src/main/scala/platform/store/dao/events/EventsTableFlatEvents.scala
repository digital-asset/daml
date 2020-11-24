// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._

private[events] object EventsTableFlatEvents {

  private val createdFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Created]] =
    EventsTable.createdEventRow map {
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
    EventsTable.archivedEventRow map {
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
    SQL"""select #$selectColumns, array[$requestingParty] as event_witnesses,
                 case when submitters = array[$requestingParty] then command_id else '' end as command_id
          from participant_events
          join parameters on participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive
          where transaction_id = $transactionId and #$witnessesWhereClause
          order by event_sequential_id"""
  }

  private def multiPartyLookup(sqlFunctions: SqlFunctions)(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", requestingParties)
    val submittersInPartiesClause =
      sqlFunctions.arrayIntersectionWhereClause("submitters", requestingParties)
    SQL"""select #$selectColumns, flat_event_witnesses as event_witnesses,
                 case when #$submittersInPartiesClause then command_id else '' end as command_id
          from participant_events
          join parameters on participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive
          where transaction_id = $transactionId and #$witnessesWhereClause
          group by (#$groupByColumns)
          order by event_sequential_id"""
  }

  private def getFlatTransactionsQueries(sqlFunctions: SqlFunctions) =
    new EventsTableFlatEventsRangeQueries.GetTransactions(
      selectColumns = selectColumns,
      sqlFunctions = sqlFunctions,
    )

  def preparePagedGetFlatTransactions(sqlFunctions: SqlFunctions)(
      range: EventsRange[Long],
      filter: FilterRelation,
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.FlatEvent]]] =
    getFlatTransactionsQueries(sqlFunctions)(
      range,
      filter,
      pageSize,
    )

  private def getActiveContractsQueries(sqlFunctions: SqlFunctions) =
    new EventsTableFlatEventsRangeQueries.GetActiveContracts(
      selectColumns = selectColumns,
      sqlFunctions = sqlFunctions,
    )

  def preparePagedGetActiveContracts(sqlFunctions: SqlFunctions)(
      range: EventsRange[(Offset, Long)],
      filter: FilterRelation,
      pageSize: Int
  ): SqlSequence[Vector[EventsTable.Entry[Raw.FlatEvent]]] =
    getActiveContractsQueries(sqlFunctions)(
      range,
      filter,
      pageSize
    )
}
