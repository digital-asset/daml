// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.digitalasset.ledger.TransactionId
import com.digitalasset.ledger.api.v1.event.Event

private[events] trait EventsTableFlatEvents { this: EventsTable =>

  private val createdFlatEventParser: RowParser[Entry[Event]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templatePackageId ~ templateName ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Event(
            Event.Event.Created(
              createdEvent(
                eventId = eventId,
                contractId = contractId,
                templatePackageId = templatePackageId,
                templateName = templateName,
                createArgument = createArgument,
                createSignatories = createSignatories,
                createObservers = createObservers,
                createAgreementText = createAgreementText.orElse(Some("")),
                createKeyValue = createKeyValue,
                eventWitnesses = eventWitnesses,
              )
            )
          )
        )
    }

  private val archivedFlatEventParser: RowParser[Entry[Event]] =
    archivedEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templatePackageId ~ templateName ~ commandId ~ workflowId ~ eventWitnesses =>
        Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Event(
            Event.Event.Archived(
              archivedEvent(
                eventId = eventId,
                contractId = contractId,
                templatePackageId = templatePackageId,
                templateName = templateName,
                eventWitnesses = eventWitnesses,
              )
            )
          )
        )
    }

  val flatEventParser: RowParser[Entry[Event]] = createdFlatEventParser | archivedFlatEventParser

  def prepareLookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] = {
    val tx: String = transactionId
    val ps: Set[String] = requestingParties.asInstanceOf[Set[String]]
    SQL"""select
            event_offset,
            transaction_id,
            ledger_effective_time,
            case when submitter in ($ps) then command_id else '' end as command_id,
            workflow_id,
            participant_events.event_id,
            contract_id,
            template_package_id,
            template_name,
            create_argument,
            create_signatories,
            create_observers,
            create_agreement_text,
            create_key_value,
            array_agg(event_witness) as event_witnesses
          from participant_events
          natural join participant_event_flat_transaction_witnesses
          where transaction_id = $tx and event_witness in ($ps)
          group by (
            event_offset,
            transaction_id,
            ledger_effective_time,
            command_id,
            workflow_id,
            participant_events.event_id,
            contract_id,
            template_package_id,
            template_name,
            create_argument,
            create_signatories,
            create_observers,
            create_agreement_text,
            create_key_value
          )
       """
  }
}
