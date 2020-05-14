// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._

private[events] trait EventsTableTreeEvents { this: EventsTable =>

  private val createdTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
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
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseResult ~ exerciseActors ~ exerciseChildEventIds =>
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
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

  private val witnessesAggregation =
    "array_agg(event_witness) as event_witnesses"

  private val treeEventsTable =
    "participant_events natural join participant_event_transaction_tree_witnesses"

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

  def prepareLookupTransactionTreeById(
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
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from #$treeEventsTable where transaction_id = $transactionId and event_witness = $requestingParty order by node_index asc"

  private def multiPartyLookup(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] =
    SQL"select #$selectColumns, #$witnessesAggregation, case when submitter in ($requestingParties) then command_id else '' end as command_id from #$treeEventsTable where transaction_id = $transactionId and event_witness in ($requestingParties) group by (#$groupByColumns) order by node_index asc"

  def preparePagedGetTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row] =
    route(requestingParties)(
      single = singlePartyTrees(startExclusive, endInclusive, _, pageSize, rowOffset),
      multi = multiPartyTrees(startExclusive, endInclusive, _, pageSize, rowOffset),
    )

  private def singlePartyTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParty: Party,
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row] =
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from #$treeEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and event_witness = $requestingParty order by (#$orderByColumns) limit $pageSize offset $rowOffset"

  private def multiPartyTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row] =
    SQL"select #$selectColumns, #$witnessesAggregation, case when submitter in ($requestingParties) then command_id else '' end as command_id from #$treeEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and event_witness in ($requestingParties) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

}
