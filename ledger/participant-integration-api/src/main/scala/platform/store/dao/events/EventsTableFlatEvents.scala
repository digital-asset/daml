// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._

import scala.collection.compat.immutable.ArraySeq

private[events] object EventsTableFlatEvents {

  private val createdFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Created]] =
    EventsTable.createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~
          templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createArgumentCompression ~
          createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyValueCompression =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
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
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(createSignatories),
            createObservers = ArraySeq.unsafeWrapArray(createObservers),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
        )
    }

  private val archivedFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Archived]] =
    EventsTable.archivedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
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
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
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
      "create_argument_compression",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
      "create_key_value_compression",
    ).mkString(", ")

  private val selectColumnsForACS =
    Seq(
      "active_cs.event_offset",
      "active_cs.transaction_id",
      "active_cs.node_index",
      "active_cs.event_sequential_id",
      "active_cs.ledger_effective_time",
      "active_cs.workflow_id",
      "active_cs.event_id",
      "active_cs.contract_id",
      "active_cs.template_id",
      "active_cs.create_argument",
      "active_cs.create_argument_compression",
      "active_cs.create_signatories",
      "active_cs.create_observers",
      "active_cs.create_agreement_text",
      "active_cs.create_key_value",
      "active_cs.create_key_value_compression",
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
      // TODO varchars are not texts according to postgreSQL - we need to do lot of casting. since these are primarlily the same I suggest for the final approach to pick either and aligne old and new code @simon@
      sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", requestingParty)
    SQL"""select #$selectColumns, array[$requestingParty] as event_witnesses,
                 case when submitters = array[$requestingParty]::text[] then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause and event_kind != 0 -- we do not want to fetch divulgence events
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
    // TODO why groupby??? is it okay to remove?
    SQL"""select #$selectColumns, flat_event_witnesses as event_witnesses,
                 case when #$submittersInPartiesClause then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause and event_kind != 0 -- we do not want to fetch divulgence events
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
      selectColumns = selectColumnsForACS,
      sqlFunctions = sqlFunctions,
    )

  def preparePagedGetActiveContracts(sqlFunctions: SqlFunctions)(
      range: EventsRange[(Offset, Long)],
      filter: FilterRelation,
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.FlatEvent]]] =
    getActiveContractsQueries(sqlFunctions)(
      range,
      filter,
      pageSize,
    )
}
