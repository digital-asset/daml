// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser.{array, binaryStream, bool, str}
import anorm.{RowParser, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.{
  TreeEvent,
  Transaction => ApiTransaction,
  TransactionTree => ApiTransactionTree
}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionsResponse,
}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.api.v1.event.EventOps.TreeEventOps
import com.digitalasset.platform.index.TransactionConversion
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.store.Conversions.{instant, offset}
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
    with EventsTableTreeEvents

private[events] trait EventsTable {

  case class Entry[E](
      eventOffset: Offset,
      transactionId: String,
      ledgerEffectiveTime: Instant,
      commandId: String,
      workflowId: String,
      event: E,
  )

  object Entry {

    private def instantToTimestamp(t: Instant): Timestamp =
      Timestamp(seconds = t.getEpochSecond, nanos = t.getNano)

    private def flatTransaction[R](makeResponse: ApiTransaction => R)(
        events: Seq[Entry[Event]],
    ): Option[R] =
      events.headOption.flatMap { first =>
        val flatEvents =
          TransactionConversion.removeTransient(events.iterator.map(_.event).toVector)
        if (flatEvents.nonEmpty || first.commandId.nonEmpty)
          Some(
            makeResponse(
              ApiTransaction(
                transactionId = first.transactionId,
                commandId = first.commandId,
                effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
                workflowId = first.workflowId,
                offset = ApiOffset.toApiString(first.eventOffset),
                events = flatEvents,
              )
            )
          )
        else None
      }

    def toGetTransactionsResponse(
        events: Vector[Entry[Event]],
    ): List[GetTransactionsResponse] =
      flatTransaction(tx => GetTransactionsResponse(Seq(tx)))(events).toList

    def toGetFlatTransactionResponse(
        events: List[Entry[Event]],
    ): Option[GetFlatTransactionResponse] =
      flatTransaction(tx => GetFlatTransactionResponse(Some(tx)))(events)

    def toTransactionTree(events: List[Entry[TreeEvent]]): Option[GetTransactionResponse] =
      events.headOption.map(
        first => {
          val (eventsById, rootEventIds) = treeOf(events)
          GetTransactionResponse(
            transaction = Some(
              ApiTransactionTree(
                transactionId = first.transactionId,
                commandId = first.commandId,
                workflowId = first.workflowId,
                effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
                offset = ApiOffset.toApiString(first.eventOffset),
                eventsById = eventsById,
                rootEventIds = rootEventIds,
                traceContext = None,
              ))
          )
        }
      )

    private def treeOf(events: List[Entry[TreeEvent]]): (Map[String, TreeEvent], Seq[String]) = {

      // The identifiers of all visible events in this transactions, preserving
      // the order in which they are retrieved from the index
      val visible = events.map(_.event.eventId)

      // All events in this transaction by their identifier, with their children
      // filtered according to those visible for this request
      val eventsById =
        events.iterator
          .map(_.event)
          .map(e => e.eventId -> e.filterChildEventIds(visible.toSet))
          .toMap

      // All event identifiers that appear as a child of another item in this response
      val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

      // The roots for this request are all visible items
      // that are not a child of some other visible item
      val rootEventIds = visible.filterNot(children)

      (eventsById, rootEventIds)

    }

  }

  private type SharedRow =
    Offset ~ String ~ String ~ String ~ Instant ~ String ~ String ~ Option[String] ~ Option[String] ~ Array[
      String]
  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      str("event_id") ~
      str("contract_id") ~
      instant("ledger_effective_time") ~
      str("template_package_id") ~
      str("template_name") ~
      str("command_id").? ~
      str("workflow_id").? ~
      array[String]("event_witnesses")

  protected type CreatedEventRow =
    SharedRow ~ InputStream ~ Array[String] ~ Array[String] ~ Option[String] ~ Option[InputStream]
  protected val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      binaryStream("create_argument") ~
      array[String]("create_signatories") ~
      array[String]("create_observers") ~
      str("create_agreement_text").? ~
      binaryStream("create_key_value").?

  protected type ExercisedEventRow =
    SharedRow ~ Boolean ~ String ~ InputStream ~ Option[InputStream] ~ Array[String] ~ Array[String]
  protected val exercisedEventRow: RowParser[ExercisedEventRow] =
    sharedRow ~
      bool("exercise_consuming") ~
      str("exercise_choice") ~
      binaryStream("exercise_argument") ~
      binaryStream("exercise_result").? ~
      array[String]("exercise_actors") ~
      array[String]("exercise_child_event_ids")

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
      agreementText = createAgreementText.orElse(Some("")),
    )

  protected def exercisedEvent(
      eventId: String,
      contractId: String,
      templatePackageId: String,
      templateName: String,
      exerciseConsuming: Boolean,
      exerciseChoice: String,
      exerciseArgument: InputStream,
      exerciseResult: Option[InputStream],
      exerciseActors: Array[String],
      exerciseChildEventIds: Array[String],
      eventWitnesses: Array[String],
  ): ExercisedEvent =
    ExercisedEvent(
      eventId = eventId,
      contractId = contractId,
      templateId = Some(templateId(templatePackageId, templateName)),
      choice = exerciseChoice,
      choiceArgument = Some(
        LfEngineToApi.assertOrRuntimeEx(
          failureContext = s"attempting to deserialize persisted exercise argument to value",
          LfEngineToApi
            .lfVersionedValueToApiValue(
              verbose = true,
              value = deserialize(exerciseArgument),
            ),
        )
      ),
      actingParties = exerciseActors,
      consuming = exerciseConsuming,
      witnessParties = eventWitnesses,
      childEventIds = exerciseChildEventIds,
      exerciseResult = exerciseResult.map(
        key =>
          LfEngineToApi.assertOrRuntimeEx(
            failureContext = s"attempting to deserialize persisted exercise result to value",
            LfEngineToApi
              .lfVersionedValueToApiValue(
                verbose = true,
                value = deserialize(key),
              ),
        )
      ),
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
