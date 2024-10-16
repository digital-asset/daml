// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.trace_context.TraceContext as DamlTraceContext
import com.daml.ledger.api.v2.transaction.{
  Transaction as ApiTransaction,
  TransactionTree as ApiTransactionTree,
  TreeEvent,
}
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.Entry
import com.digitalasset.canton.platform.store.utils.EventOps.TreeEventOps

object EventsTable {

  object TransactionConversions {

    private def extractTraceContext[EventT](
        events: Vector[Entry[EventT]]
    ): Option[DamlTraceContext] =
      events
        .map(_.traceContext)
        .collectFirst { case Some(tc) => tc }
        .map(DamlTraceContext.parseFrom)

    private def flatTransaction(events: Vector[Entry[Event]]): Option[ApiTransaction] =
      events.headOption.flatMap { first =>
        val flatEvents =
          TransactionConversion.removeTransient(events.iterator.map(_.event).toVector)

        // Allows emitting flat transactions with no events, a use-case needed
        // for the functioning of Daml triggers.
        // (more details in https://github.com/digital-asset/daml/issues/6975)
        if (flatEvents.nonEmpty || first.commandId.nonEmpty)
          Some(
            ApiTransaction(
              updateId = first.updateId,
              commandId = first.commandId.getOrElse(""),
              effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
              workflowId = first.workflowId.getOrElse(""),
              offset = ApiOffset.assertFromStringToLong(first.offset),
              events = flatEvents,
              domainId = first.domainId,
              traceContext = extractTraceContext(events),
              recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            )
          )
        else None
      }

    def toGetTransactionsResponse(
        events: Vector[Entry[Event]]
    ): List[(Long, GetUpdatesResponse)] =
      flatTransaction(events).toList.map(tx =>
        tx.offset -> GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(tx))
          .withPrecomputedSerializedSize()
      )

    def toGetFlatTransactionResponse(
        events: Vector[Entry[Event]]
    ): Option[GetTransactionResponse] =
      flatTransaction(events).map(tx => GetTransactionResponse(Some(tx)))

    private def treeOf(
        events: Vector[Entry[TreeEvent]]
    ): (Map[String, TreeEvent], Vector[String], Option[DamlTraceContext]) = {

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

      (eventsById, rootEventIds, extractTraceContext(events))

    }

    private def transactionTree(
        events: Vector[Entry[TreeEvent]]
    ): Option[ApiTransactionTree] =
      events.headOption.map { first =>
        val (eventsById, rootEventIds, traceContext) = treeOf(events)
        ApiTransactionTree(
          updateId = first.updateId,
          commandId = first.commandId.getOrElse(""),
          workflowId = first.workflowId.getOrElse(""),
          effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
          offset = ApiOffset.assertFromStringToLong(first.offset),
          eventsById = eventsById,
          rootEventIds = rootEventIds,
          domainId = first.domainId,
          traceContext = traceContext,
          recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
        )
      }

    def toGetTransactionTreesResponse(
        events: Vector[Entry[TreeEvent]]
    ): List[(Long, GetUpdateTreesResponse)] =
      transactionTree(events).toList.map(tx =>
        tx.offset -> GetUpdateTreesResponse(GetUpdateTreesResponse.Update.TransactionTree(tx))
          .withPrecomputedSerializedSize()
      )

    def toGetTransactionResponse(
        events: Vector[Entry[TreeEvent]]
    ): Option[GetTransactionTreeResponse] =
      transactionTree(events).map(tx => GetTransactionTreeResponse(Some(tx)))

  }

}
