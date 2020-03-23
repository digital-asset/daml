// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.digitalasset.ledger.TransactionId
import com.digitalasset.ledger.api.v1.transaction.TreeEvent

private[events] trait EventsTableTreeEvents { this: EventsTable =>

  private val createdTreeEventParser: RowParser[Entry[TreeEvent]] =
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
              )
            )
          )
        )
    }

  private val exercisedTreeEventParser: RowParser[Entry[TreeEvent]] =
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
              )
            )
          )
        )
    }

  val treeEventParser
    : RowParser[Entry[TreeEvent]] = createdTreeEventParser | exercisedTreeEventParser

  def prepareLookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val tx: String = transactionId
    val ps: Set[String] = requestingParties.asInstanceOf[Set[String]]
    SQL"""select
            event_offset,
            transaction_id,
            node_index,
            participant_events.event_id,
            contract_id,
            ledger_effective_time,
            template_package_id,
            template_name,
            case when submitter in ($ps) then command_id else '' end as command_id,
            workflow_id,
            create_argument,
            create_signatories,
            create_observers,
            create_agreement_text,
            create_key_value,
            exercise_consuming,
            exercise_choice,
            exercise_argument,
            exercise_result,
            exercise_actors,
            exercise_child_event_ids,
            array_agg(event_witness) as event_witnesses
          from participant_events
          natural join (
            select event_id, event_witness from participant_event_flat_transaction_witnesses
            union
            select event_id, event_witness from participant_event_witnesses_complement
          ) as participant_event_witnesses
          where transaction_id = $tx and event_witness in ($ps)
          group by (
            event_offset,
            transaction_id,
            node_index,
            participant_events.event_id,
            contract_id,
            ledger_effective_time,
            template_package_id,
            template_name,
            command_id,
            workflow_id,
            application_id,
            submitter,
            create_argument,
            create_signatories,
            create_observers,
            create_agreement_text,
            create_key_value,
            exercise_consuming,
            exercise_choice,
            exercise_argument,
            exercise_result,
            exercise_actors,
            exercise_child_event_ids
          )
          order by node_index asc
       """
  }

}
