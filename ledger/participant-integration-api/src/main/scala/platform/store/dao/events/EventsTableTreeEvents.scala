// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.TransactionId
import com.daml.platform.store.Conversions._

private[events] trait EventsTableTreeEvents { this: EventsTable =>

  private val createdTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Created]] =
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
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseResult ~ exerciseActors ~ exerciseChildEventIds =>
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
    "event_sequential_id",
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
  ): SimpleSql[Row] = {
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and da_do_varchar_arrays_intersect(tree_event_witnesses::varchar[], array[$requestingParty]) order by node_index asc"
  }

  private def multiPartyLookup(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    SQL"select #$selectColumns, da_varchar_arrays_intersection(tree_event_witnesses::varchar[], $requestingParties) as event_witnesses, case when submitter in ($requestingParties) then command_id else '' end as command_id from participant_events where transaction_id = $transactionId and da_do_varchar_arrays_intersect(tree_event_witnesses::varchar[], $requestingParties) group by (#$groupByColumns) order by node_index asc"
  }

  def preparePagedGetTransactionTrees(
      eventsRange: EventsRange[Long],
      requestingParties: Set[Party],
      pageSize: Int,
  ): SimpleSql[Row] =
    route(requestingParties)(
      single = singlePartyTrees(eventsRange, _, pageSize),
      multi = multiPartyTrees(eventsRange, _, pageSize),
    )

  private def singlePartyTrees(
      range: EventsRange[Long],
      requestingParty: Party,
      pageSize: Int,
  ): SimpleSql[Row] = {
    SQL"select #$selectColumns, array[$requestingParty] as event_witnesses, case when submitter = $requestingParty then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and da_do_varchar_arrays_intersect(tree_event_witnesses::varchar[], array[$requestingParty]) order by event_sequential_id limit $pageSize"
  }

  private def multiPartyTrees(
      range: EventsRange[Long],
      requestingParties: Set[Party],
      pageSize: Int,
  ): SimpleSql[Row] = {
    SQL"select #$selectColumns, da_varchar_arrays_intersection(tree_event_witnesses::varchar[], $requestingParties) as event_witnesses, case when submitter in ($requestingParties) then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and da_do_varchar_arrays_intersect(tree_event_witnesses::varchar[], $requestingParties) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
  }

}
