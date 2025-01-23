// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.state_service.ParticipantPermission.*
import com.daml.ledger.api.v2.topology_transaction.{
  ParticipantAuthorizationChanged,
  ParticipantAuthorizationRevoked,
  TopologyEvent,
  TopologyTransaction,
}
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
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawParticipantAuthorization,
}
import com.digitalasset.canton.platform.store.utils.EventOps.TreeEventOps

object EventsTable {

  object TransactionConversions {

    private def extractTraceContext[EventT](
        events: Seq[Entry[EventT]]
    ): Option[DamlTraceContext] =
      events.iterator
        .map(_.traceContext)
        .collectFirst { case Some(tc) => tc }
        .map(DamlTraceContext.parseFrom)

    private def flatTransaction(events: Seq[Entry[Event]]): Option[ApiTransaction] =
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
              offset = first.offset,
              events = flatEvents,
              synchronizerId = first.synchronizerId,
              traceContext = extractTraceContext(events),
              recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            )
          )
        else None
      }

    def toGetTransactionsResponse(
        events: Seq[Entry[Event]]
    ): List[(Long, GetUpdatesResponse)] =
      flatTransaction(events).toList.map(tx =>
        tx.offset -> GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(tx))
          .withPrecomputedSerializedSize()
      )

    def toTopologyTransaction(
        events: Vector[RawParticipantAuthorization]
    ): Option[(Offset, TopologyTransaction)] =
      events.headOption.map { first =>
        first.offset ->
          TopologyTransaction(
            updateId = first.updateId,
            events = events
              .map(event =>
                event.participant_permission match {
                  case Submission => (event, true, PARTICIPANT_PERMISSION_SUBMISSION)
                  case Confirmation => (event, true, PARTICIPANT_PERMISSION_CONFIRMATION)
                  case Observation => (event, true, PARTICIPANT_PERMISSION_OBSERVATION)
                  case Revoked => (event, false, PARTICIPANT_PERMISSION_UNSPECIFIED)
                }
              )
              .map {
                case (event, true, level) =>
                  TopologyEvent(
                    TopologyEvent.Event.ParticipantAuthorizationChanged(
                      ParticipantAuthorizationChanged(
                        partyId = event.partyId,
                        participantId = event.participantId,
                        participantPermission = level,
                      )
                    )
                  )
                case (event, false, _) =>
                  TopologyEvent(
                    TopologyEvent.Event.ParticipantAuthorizationRevoked(
                      ParticipantAuthorizationRevoked(
                        partyId = event.partyId,
                        participantId = event.participantId,
                      )
                    )
                  )
              },
            offset = first.offset.unwrap,
            traceContext = first.traceContext.map(DamlTraceContext.parseFrom),
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
          )
      }

    def toGetFlatTransactionResponse(
        events: Seq[Entry[Event]]
    ): Option[GetTransactionResponse] =
      flatTransaction(events).map(tx => GetTransactionResponse(Some(tx)))

    private def treeOf(
        events: Seq[Entry[TreeEvent]]
    ): (Map[Int, TreeEvent], Option[DamlTraceContext]) = {

      // All events in this transaction by their identifier, with their children
      // filtered according to those visible for this request
      val eventsById =
        events.iterator
          .map(_.event)
          .map(e => e.nodeId -> e)
          .toMap

      (eventsById, extractTraceContext(events))

    }

    private def transactionTree(
        events: Seq[Entry[TreeEvent]]
    ): Option[ApiTransactionTree] =
      events.headOption.map { first =>
        val (eventsById, traceContext) = treeOf(events)
        ApiTransactionTree(
          updateId = first.updateId,
          commandId = first.commandId.getOrElse(""),
          workflowId = first.workflowId.getOrElse(""),
          effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
          offset = first.offset,
          eventsById = eventsById,
          synchronizerId = first.synchronizerId,
          traceContext = traceContext,
          recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
        )
      }

    def toGetTransactionTreesResponse(
        events: Seq[Entry[TreeEvent]]
    ): List[(Long, GetUpdateTreesResponse)] =
      transactionTree(events).toList.map(tx =>
        tx.offset -> GetUpdateTreesResponse(GetUpdateTreesResponse.Update.TransactionTree(tx))
          .withPrecomputedSerializedSize()
      )

    def toGetTransactionResponse(
        events: Seq[Entry[TreeEvent]]
    ): Option[GetTransactionTreeResponse] =
      transactionTree(events).map(tx => GetTransactionTreeResponse(Some(tx)))

  }

}
