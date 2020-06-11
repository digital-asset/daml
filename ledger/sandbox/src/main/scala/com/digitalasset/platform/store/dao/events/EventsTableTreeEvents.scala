// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.EventsTableQueries.previousOffsetWhereClauseValues
import scalaz.syntax.std.option._

private[events] trait EventsTableTreeEvents { this: EventsTable =>

  private val createdTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Created(
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

  private val exercisedTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Exercised]] =
    exercisedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseResult ~ exerciseActors ~ exerciseChildEventIds =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
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
            exerciseResult = exerciseResult,
            exerciseActors = exerciseActors,
            exerciseChildEventIds = exerciseChildEventIds,
            eventWitnesses = eventWitnesses,
          )
        )
    }

  val rawTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent]] =
    createdTreeEventParser | exercisedTreeEventParser

  private val selectColumns = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "participant_events.event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "create_argument",
    "create_signatories",
    "create_observers",
    "create_agreement_text",
    "create_key_value",
    "exercise_consuming",
    "exercise_choice",
    "exercise_argument",
    "exercise_result",
    "exercise_actors",
    "exercise_child_event_ids",
  ).mkString(", ")

  private val groupByColumns = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "participant_events.event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "command_id",
    "workflow_id",
    "application_id",
    "submitter",
    "create_argument",
    "create_signatories",
    "create_observers",
    "create_agreement_text",
    "create_key_value",
    "exercise_consuming",
    "exercise_choice",
    "exercise_argument",
    "exercise_result",
    "exercise_actors",
    "exercise_child_event_ids",
  ).mkString(", ")

  private val orderByColumns =
    Seq("event_offset", "transaction_id", "node_index").mkString(", ")

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
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and #$witnessesWhereClause order by node_index asc"
  }

  private def multiPartyLookup(sqlFunctions: SqlFunctions)(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParties)
    val filteredWitnesses =
      sqlFunctions.arrayIntersectionValues("tree_event_witnesses", requestingParties)
    SQL"select #$selectColumns, #$filteredWitnesses as event_witnesses, case when submitter in ($requestingParties) then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and #$witnessesWhereClause group by (#$groupByColumns) order by node_index asc"
  }

  def preparePagedGetTransactionTrees(sqlFunctions: SqlFunctions)(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SqlSequence[Vector[EventsTable.Entry[Raw.TreeEvent]]] =
    route(requestingParties)(
      single = singlePartyTrees(sqlFunctions)(
        startExclusive,
        endInclusive,
        _,
        pageSize,
        previousEventNodeIndex),
      multi = multiPartyTrees(sqlFunctions)(
        startExclusive,
        endInclusive,
        _,
        pageSize,
        previousEventNodeIndex),
    )

  private def singlePartyTrees(sqlFunctions: SqlFunctions)(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParty: Party,
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SqlSequence[Vector[EventsTable.Entry[Raw.TreeEvent]]] = {
    val (prevOffset, prevNodeIndex) =
      previousOffsetWhereClauseValues(startExclusive, previousEventNodeIndex)
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParty)
    for {
      foundStartingRow <- SqlSequence(
        SQL"""select min(row_id) row_id from (
                select min(row_id) row_id
                from participant_events
                where event_offset > $startExclusive
                  and event_offset <= $endInclusive
                  and #$witnessesWhereClause
               union all
                -- this sub-query needs the index participant_event_event_offset_idx to run fast
                select min(row_id) row_id
                from participant_events
                where
                  event_offset = $prevOffset and node_index > $prevNodeIndex
                  and #$witnessesWhereClause
              ) rows""".withFetchSize(Some(1)),
        SqlParser.int("row_id").?.single
      )
      bestEffortNonEmpty <- foundStartingRow.cata(
        some = startingRow =>
          SqlSequence
            .vector(
              SQL"""select #$selectColumns, array[$requestingParty] as event_witnesses,
                           case when submitter = $requestingParty then command_id else '' end as command_id
                    from participant_events
                    where
                      participant_events.row_id >= $startingRow
                      and participant_events.row_id < ${startingRow + pageSize}
                      and event_offset <= $endInclusive and #$witnessesWhereClause
                    order by (participant_events.row_id)"""
                .withFetchSize(Some(pageSize)),
              rawTreeEventParser
            )
            .flatMap { boundSelection =>
              if (boundSelection.isEmpty)
                SqlSequence(
                  SQL"""select #$selectColumns, array[$requestingParty] as event_witnesses,
                               case when submitter = $requestingParty then command_id else '' end as command_id
                        from participant_events
                        where
                          participant_events.row_id >= ${startingRow + pageSize}
                          and event_offset <= $endInclusive and #$witnessesWhereClause
                        order by (participant_events.row_id) limit 1"""
                    .withFetchSize(Some(1)),
                  rawTreeEventParser.singleOpt map (_.toList.toVector)
                )
              else SqlSequence point boundSelection
          },
        none = SqlSequence point Vector.empty
      )
    } yield bestEffortNonEmpty
  }

  private def multiPartyTrees(sqlFunctions: SqlFunctions)(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SqlSequence[Vector[EventsTable.Entry[Raw.TreeEvent]]] = {
    val (prevOffset, prevNodeIndex) =
      previousOffsetWhereClauseValues(startExclusive, previousEventNodeIndex)
    val witnessesWhereClause =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", requestingParties)
    val filteredWitnesses =
      sqlFunctions.arrayIntersectionValues("tree_event_witnesses", requestingParties)
    SqlSequence.vector(
      SQL"select #$selectColumns, #$filteredWitnesses as event_witnesses, case when submitter in ($requestingParties) then command_id else '' end as command_id from participant_events where (event_offset > $startExclusive or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= $endInclusive and #$witnessesWhereClause group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
        .withFetchSize(Some(pageSize)),
      rawTreeEventParser
    )
  }

}
