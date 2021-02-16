// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._

import scala.collection.compat.immutable.ArraySeq

private[events] object EventsTableTreeEvents {

  private val createdTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Created]] =
    EventsTable.createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createArgumentCompression ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyValueCompression =>
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
          event = Raw.TreeEvent.Created(
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

  private val exercisedTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Exercised]] =
    EventsTable.exercisedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~ exerciseChildEventIds =>
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
          event = Raw.TreeEvent.Exercised(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            exerciseConsuming = exerciseConsuming,
            exerciseChoice = exerciseChoice,
            exerciseArgument = exerciseArgument,
            exerciseArgumentCompression = exerciseArgumentCompression,
            exerciseResult = exerciseResult,
            exerciseResultCompression = exerciseResultCompression,
            exerciseActors = ArraySeq.unsafeWrapArray(exerciseActors),
            exerciseChildEventIds = ArraySeq.unsafeWrapArray(exerciseChildEventIds),
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
        )
    }

  val rawTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent]] =
    createdTreeEventParser | exercisedTreeEventParser

  private val selectColumns = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "event_sequential_id",
    "participant_events.event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "create_argument",
    "create_argument_compression",
    "create_signatories",
    "create_observers",
    "create_agreement_text",
    "create_key_value",
    "create_key_value_compression",
    "event_kind = 20 as exercise_consuming", // since we removed the exercise_consuming column, we know derive the informationf from the event_kind
    "exercise_choice",
    "exercise_argument",
    "exercise_argument_compression",
    "exercise_result",
    "exercise_result_compression",
    "exercise_actors",
    "exercise_child_event_ids",
  ).mkString(", ")

  def prepareLookupTransactionTreeById(sqlFunctions: SqlFunctions)(
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
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParty)
    SQL"""select #$selectColumns, array[$requestingParty] as event_witnesses,
                 case when submitters = array[$requestingParty]::text[] then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause
          order by node_index asc"""
  }

  private def multiPartyLookup(sqlFunctions: SqlFunctions)(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParties)
    val filteredWitnesses =
      sqlFunctions.arrayIntersectionValues("tree_event_witnesses", requestingParties)
    val submittersInPartiesClause =
      sqlFunctions.arrayIntersectionWhereClause("submitters", requestingParties)
    SQL"""select #$selectColumns, #$filteredWitnesses as event_witnesses,
                 case when #$submittersInPartiesClause then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause
          order by node_index asc"""
  }

  def preparePagedGetTransactionTrees(sqlFunctions: SqlFunctions)(
      eventsRange: EventsRange[Long],
      requestingParties: Set[Party],
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.TreeEvent]]] =
    route(requestingParties)(
      single = singlePartyTrees(sqlFunctions)(eventsRange, _, pageSize),
      multi = multiPartyTrees(sqlFunctions)(eventsRange, _, pageSize),
    )

  private def singlePartyTrees(sqlFunctions: SqlFunctions)(
      range: EventsRange[Long],
      requestingParty: Party,
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.TreeEvent]]] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParty)
    EventsRange.readPage(
      read = (range, limitExpr) => SQL"""
        select #$selectColumns, array[$requestingParty] as event_witnesses,
               case when submitters = array[$requestingParty]::text[] then command_id else '' end as command_id
        from participant_events
        where event_sequential_id > ${range.startExclusive}
              and event_sequential_id <= ${range.endInclusive}
              and #$witnessesWhereClause
        order by event_sequential_id #$limitExpr""",
      rawTreeEventParser,
      range,
      pageSize,
    )
  }

  private def multiPartyTrees(sqlFunctions: SqlFunctions)(
      range: EventsRange[Long],
      requestingParties: Set[Party],
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.TreeEvent]]] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParties)
    val filteredWitnesses =
      sqlFunctions.arrayIntersectionValues("tree_event_witnesses", requestingParties)
    val submittersInPartiesClause =
      sqlFunctions.arrayIntersectionWhereClause("submitters", requestingParties)
    EventsRange.readPage(
      read = (range, limitExpr) => SQL"""
        select #$selectColumns, #$filteredWitnesses as event_witnesses,
               case when #$submittersInPartiesClause then command_id else '' end as command_id
        from participant_events
        where event_sequential_id > ${range.startExclusive}
              and event_sequential_id <= ${range.endInclusive}
              and #$witnessesWhereClause
        order by event_sequential_id #$limitExpr""",
      rawTreeEventParser,
      range,
      pageSize,
    )
  }

}
