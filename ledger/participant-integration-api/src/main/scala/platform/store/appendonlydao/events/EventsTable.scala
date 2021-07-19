// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.{
  TreeEvent,
  Transaction => ApiTransaction,
  TransactionTree => ApiTransactionTree,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.offset.Offset
import com.daml.platform.ApiOffset
import com.daml.platform.api.v1.event.EventOps.{EventOps, TreeEventOps}
import com.daml.platform.index.TransactionConversion
import com.google.protobuf.timestamp.Timestamp

// TODO append-only: FIXME: move to the right place
object EventsTable {

  final case class Entry[+E](
      eventOffset: Offset,
      transactionId: String,
      nodeIndex: Int,
      eventSequentialId: Long,
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
        // Allows emitting flat transactions with no events, a use-case needed
        // for the functioning of DAML triggers.
        // (more details in https://github.com/digital-asset/daml/issues/6975)
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
        events: Vector[Entry[Event]]
    ): List[GetTransactionsResponse] =
      flatTransaction(events).toList.map(tx => GetTransactionsResponse(Seq(tx)))

    def toGetFlatTransactionResponse(
        events: Vector[Entry[Event]]
    ): Option[GetFlatTransactionResponse] =
      flatTransaction(events).map(tx => GetFlatTransactionResponse(Some(tx)))

    def toGetActiveContractsResponse(
        events: Vector[Entry[Event]]
    ): Vector[GetActiveContractsResponse] = {
      events.map {
        case entry if entry.event.isCreated =>
          GetActiveContractsResponse(
            offset = "", // only the last response will have an offset.
            workflowId = entry.workflowId,
            activeContracts = Seq(entry.event.getCreated),
          )
        case entry =>
          throw new IllegalStateException(
            s"Non-create event ${entry.event.eventId} fetched as part of the active contracts"
          )
      }
    }

    private def treeOf(
        events: Vector[Entry[TreeEvent]]
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
        events: Vector[Entry[TreeEvent]]
    ): Option[ApiTransactionTree] =
      events.headOption.map { first =>
        val (eventsById, rootEventIds) = treeOf(events)
        ApiTransactionTree(
          transactionId = first.transactionId,
          commandId = first.commandId,
          workflowId = first.workflowId,
          effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
          offset = ApiOffset.toApiString(first.eventOffset),
          eventsById = eventsById,
          rootEventIds = rootEventIds,
        )
      }

    def toGetTransactionTreesResponse(
        events: Vector[Entry[TreeEvent]]
    ): List[GetTransactionTreesResponse] =
      transactionTree(events).toList.map(tx => GetTransactionTreesResponse(Seq(tx)))

    def toGetTransactionResponse(
        events: Vector[Entry[TreeEvent]]
    ): Option[GetTransactionResponse] =
      transactionTree(events).map(tx => GetTransactionResponse(Some(tx)))

  }

}
