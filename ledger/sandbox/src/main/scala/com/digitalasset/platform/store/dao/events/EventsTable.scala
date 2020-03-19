// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import java.io.InputStream
import java.util.Date

import anorm.SqlParser._
import anorm.{RowParser, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.{Transaction => ApiTransaction}
import com.digitalasset.ledger.api.v1.transaction_service.GetFlatTransactionResponse
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.index.TransactionConversion
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.store.Conversions.offset
import com.digitalasset.platform.store.serialization.ValueSerializer.{
  deserializeValue => deserialize
}
import com.google.protobuf.timestamp.Timestamp

/**
  * Data access object for a table representing raw transactions nodes that
  * are going to be streamed off through the Ledger API. By joining these items
  * with a [[WitnessesTable]] events can be filtered based on their visibility to
  * a party.
  */
private[events] object EventsTable
    extends EventsTable
    with EventsTableInsert
    with EventsTableFlatEvents

private[events] trait EventsTable {

  case class Entry[E](
      eventOffset: Offset,
      transactionId: String,
      ledgerEffectiveTime: Date,
      commandId: String,
      workflowId: String,
      event: E,
  )

  object Entry {

    def toFlatTransaction(events: List[Entry[Event]]): Option[GetFlatTransactionResponse] = {
      events.headOption.flatMap { first =>
        val flatEvents =
          TransactionConversion.removeTransient(events.iterator.map(_.event).toVector)
        if (flatEvents.nonEmpty || first.commandId.nonEmpty)
          Some(
            GetFlatTransactionResponse(
              transaction = Some(
                ApiTransaction(
                  transactionId = first.transactionId,
                  commandId = first.commandId,
                  effectiveAt =
                    Some(Timestamp.of(seconds = first.ledgerEffectiveTime.getTime, nanos = 0)),
                  workflowId = first.workflowId,
                  offset = ApiOffset.toApiString(first.eventOffset),
                  events = flatEvents,
                )
              )
            )
          )
        else None

      }
    }

  }

  private type SharedRow =
    Offset ~ String ~ String ~ String ~ Date ~ String ~ String ~ Option[String] ~ Option[String] ~ Array[
      String]
  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      str("event_id") ~
      str("contract_id") ~
      date("ledger_effective_time") ~
      str("template_package_id") ~
      str("template_name") ~
      get[Option[String]]("command_id") ~
      get[Option[String]]("workflow_id") ~
      array[String]("event_witnesses")

  protected type CreatedEventRow =
    SharedRow ~ InputStream ~ Array[String] ~ Array[String] ~ Option[String] ~ Option[InputStream]
  protected val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      binaryStream("create_argument") ~
      array[String]("create_signatories") ~
      array[String]("create_observers") ~
      get[Option[String]]("create_agreement_text") ~
      get[Option[InputStream]]("create_key_value")

  protected type ArchiveEventRow = SharedRow
  protected val archivedEventRow: RowParser[ArchiveEventRow] = sharedRow

  private def templateId(
      templatePackageId: String,
      templateName: String,
  ): Identifier = {
    val QualifiedName(templateModuleName, templateEntityName) =
      QualifiedName.assertFromString(templateName)
    Identifier(
      packageId = templatePackageId,
      moduleName = templateModuleName.dottedName,
      entityName = templateEntityName.dottedName,
    )
  }

  protected def createdEvent(
      eventId: String,
      contractId: String,
      templatePackageId: String,
      templateName: String,
      createArgument: InputStream,
      createSignatories: Array[String],
      createObservers: Array[String],
      createAgreementText: Option[String],
      createKeyValue: Option[InputStream],
      eventWitnesses: Array[String],
  ): CreatedEvent =
    CreatedEvent(
      eventId = eventId,
      contractId = contractId,
      templateId = Some(templateId(templatePackageId, templateName)),
      contractKey = createKeyValue.map(
        key =>
          LfEngineToApi.assertOrRuntimeEx(
            failureContext = s"attempting to deserialize persisted key to value",
            LfEngineToApi
              .lfVersionedValueToApiValue(
                verbose = true,
                value = deserialize(key),
              ),
        )
      ),
      createArguments = Some(
        LfEngineToApi.assertOrRuntimeEx(
          failureContext = s"attempting to deserialize persisted create argument to record",
          LfEngineToApi
            .lfVersionedValueToApiRecord(
              verbose = true,
              recordValue = deserialize(createArgument),
            ),
        )
      ),
      witnessParties = eventWitnesses,
      signatories = createSignatories,
      observers = createObservers,
      agreementText = createAgreementText,
    )

  protected def archivedEvent(
      eventId: String,
      contractId: String,
      templatePackageId: String,
      templateName: String,
      eventWitnesses: Array[String],
  ): ArchivedEvent =
    ArchivedEvent(
      eventId = eventId,
      contractId = contractId,
      templateId = Some(templateId(templatePackageId, templateName)),
      witnessParties = eventWitnesses,
    )

}
