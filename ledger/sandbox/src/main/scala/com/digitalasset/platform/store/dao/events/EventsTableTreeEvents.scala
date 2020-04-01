// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.platform.store.Conversions._

private[events] trait EventsTableTreeEvents { this: EventsTable =>

  private def createdTreeEventParser(verbose: Boolean): RowParser[Entry[TreeEvent]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templatePackageId ~ templateName ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = TreeEvent(
            TreeEvent.Kind.Created(
              createdEvent(
                eventId = eventId,
                contractId = contractId,
                templatePackageId = templatePackageId,
                templateName = templateName,
                createArgument = createArgument,
                createSignatories = createSignatories,
                createObservers = createObservers,
                createAgreementText = createAgreementText,
                createKeyValue = createKeyValue,
                eventWitnesses = eventWitnesses,
                verbose = verbose,
              )
            )
          )
        )
    }

  private def exercisedTreeEventParser(verbose: Boolean): RowParser[Entry[TreeEvent]] =
    exercisedEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templatePackageId ~ templateName ~ commandId ~ workflowId ~ eventWitnesses ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseResult ~ exerciseActors ~ exerciseChildEventIds =>
        Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = TreeEvent(
            TreeEvent.Kind.Exercised(
              exercisedEvent(
                eventId = eventId,
                contractId = contractId,
                templatePackageId = templatePackageId,
                templateName = templateName,
                exerciseConsuming = exerciseConsuming,
                exerciseChoice = exerciseChoice,
                exerciseArgument = exerciseArgument,
                exerciseResult = exerciseResult,
                exerciseActors = exerciseActors,
                exerciseChildEventIds = exerciseChildEventIds,
                eventWitnesses = eventWitnesses,
                verbose = verbose,
              )
            )
          )
        )
    }

  private val verboseTreeEventParser: RowParser[Entry[TreeEvent]] =
    createdTreeEventParser(verbose = true) | exercisedTreeEventParser(verbose = true)

  private val succinctTreeEventParser: RowParser[Entry[TreeEvent]] =
    createdTreeEventParser(verbose = false) | exercisedTreeEventParser(verbose = false)

  def treeEventParser(verbose: Boolean): RowParser[Entry[TreeEvent]] =
    if (verbose) verboseTreeEventParser else succinctTreeEventParser

  private val selectColumns = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "participant_events.event_id",
    "contract_id",
    "ledger_effective_time",
    "template_package_id",
    "template_name",
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
    "array_agg(event_witness) as event_witnesses",
  ).mkString(", ")

  private val treeEventsTable =
    "participant_events natural join (select event_id, event_witness from participant_event_flat_transaction_witnesses union select event_id, event_witness from participant_event_witnesses_complement) as participant_event_witnesses"

  private val groupByColumns = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "participant_events.event_id",
    "contract_id",
    "ledger_effective_time",
    "template_package_id",
    "template_name",
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
    SQL"select #$selectColumns, case when submitter in ($requestingParties) then command_id else '' end as command_id from #$treeEventsTable where transaction_id = $transactionId and event_witness in ($requestingParties) group by (#$groupByColumns) order by node_index asc"

  def preparePagedGetTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row] =
    SQL"select #$selectColumns, case when submitter in ($requestingParties) then command_id else '' end as command_id from #$treeEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and event_witness in ($requestingParties) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

}
