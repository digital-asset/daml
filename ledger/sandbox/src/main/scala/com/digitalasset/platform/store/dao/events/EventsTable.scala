// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser.{array, binaryStream, bool, str}
import anorm.{RowParser, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.{
  TreeEvent,
  Transaction => ApiTransaction,
  TransactionTree => ApiTransactionTree
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.platform.ApiOffset
import com.daml.platform.api.v1.event.EventOps.{EventOps, TreeEventOps}
import com.daml.platform.index.TransactionConversion
import com.daml.platform.store.Conversions.{identifier, instant, offset}
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
    with EventsTableTreeEvents {

  final case class Entry[+E](
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

    private def flatTransaction(events: Vector[Entry[Event]]): Option[ApiTransaction] =
      events.headOption.flatMap { first =>
        val flatEvents =
          TransactionConversion.removeTransient(events.iterator.map(_.event).toVector)
        if (flatEvents.nonEmpty || first.commandId.nonEmpty)
          Some(
            ApiTransaction(
              transactionId = first.transactionId,
              commandId = first.commandId,
              effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
              workflowId = first.workflowId,
              offset = ApiOffset.toApiString(first.eventOffset),
              events = flatEvents,
            )
          )
        else None
      }

    def toGetTransactionsResponse(
        events: Vector[Entry[Event]],
    ): List[GetTransactionsResponse] =
      flatTransaction(events).toList.map(tx => GetTransactionsResponse(Seq(tx)))

    def toGetFlatTransactionResponse(
        events: Vector[Entry[Event]],
    ): Option[GetFlatTransactionResponse] =
      flatTransaction(events).map(tx => GetFlatTransactionResponse(Some(tx)))

    def toGetActiveContractsResponse(
        events: Vector[Entry[Event]],
    ): Vector[GetActiveContractsResponse] = {
      events.map {
        case entry if entry.event.isCreated =>
          GetActiveContractsResponse(
            offset = ApiOffset.toApiString(entry.eventOffset),
            workflowId = entry.workflowId,
            activeContracts = Seq(entry.event.getCreated),
            traceContext = None,
          )
        case entry =>
          throw new IllegalStateException(
            s"Non-create event ${entry.event.eventId} fetched as part of the active contracts"
          )
      }
    }

    private def treeOf(
        events: Vector[Entry[TreeEvent]],
    ): (Map[String, TreeEvent], Vector[String]) = {

      // The identifiers of all visible events in this transactions, preserving
      // the order in which they are retrieved from the index
      val visible = events.map(_.event.eventId)
      val visibleSet = visible.toSet

      // All events in this transaction by their identifier, with their children
      // filtered according to those visible for this request
      val eventsById =
        events.iterator
          .map(_.event)
          .map(e => e.eventId -> e.filterChildEventIds(visibleSet))
          .toMap

      // All event identifiers that appear as a child of another item in this response
      val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

      // The roots for this request are all visible items
      // that are not a child of some other visible item
      val rootEventIds = visible.filterNot(children)

      (eventsById, rootEventIds)

    }

    private def transactionTree(
        events: Vector[Entry[TreeEvent]],
    ): Option[ApiTransactionTree] =
      events.headOption.map(
        first => {
          val (eventsById, rootEventIds) = treeOf(events)
          ApiTransactionTree(
            transactionId = first.transactionId,
            commandId = first.commandId,
            workflowId = first.workflowId,
            effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
            offset = ApiOffset.toApiString(first.eventOffset),
            eventsById = eventsById,
            rootEventIds = rootEventIds,
            traceContext = None,
          )
        }
      )

    def toGetTransactionTreesResponse(
        events: Vector[Entry[TreeEvent]],
    ): List[GetTransactionTreesResponse] =
      transactionTree(events).toList.map(tx => GetTransactionTreesResponse(Seq(tx)))

    def toGetTransactionResponse(
        events: Vector[Entry[TreeEvent]],
    ): Option[GetTransactionResponse] =
      transactionTree(events).map(tx => GetTransactionResponse(Some(tx)))

  }

}

private[events] trait EventsTable {

  private type SharedRow =
    Offset ~ String ~ String ~ String ~ Instant ~ Identifier ~ Option[String] ~ Option[String] ~ Array[
      String]
  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      str("event_id") ~
      str("contract_id") ~
      instant("ledger_effective_time") ~
      identifier("template_id") ~
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

}
