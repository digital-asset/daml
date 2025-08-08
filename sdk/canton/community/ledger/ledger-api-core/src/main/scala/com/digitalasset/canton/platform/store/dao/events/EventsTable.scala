// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.state_service.ParticipantPermission
import com.daml.ledger.api.v2.state_service.ParticipantPermission.*
import com.daml.ledger.api.v2.topology_transaction.{
  ParticipantAuthorizationAdded,
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
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawParticipantAuthorization,
}
import com.digitalasset.canton.platform.store.utils.EventOps.TreeEventOps
import com.google.protobuf.ByteString

import scala.annotation.nowarn

object EventsTable {

  object TransactionConversions {

    private def extractTraceContext[EventT](
        events: Seq[Entry[EventT]]
    ): Option[DamlTraceContext] =
      events.iterator
        .map(_.traceContext)
        .collectFirst { case Some(tc) => tc }
        .map(DamlTraceContext.parseFrom)

    def toTransaction(
        entries: Seq[Entry[Event]],
        transactionShape: TransactionShape,
    ): Option[ApiTransaction] =
      entries.headOption.flatMap { first =>
        val events = entries.iterator.map(_.event).toVector
        transactionShape match {
          case AcsDelta =>
            Option.when(events.nonEmpty)(
              toApiTransaction(
                first = first,
                events = events,
                traceContext = extractTraceContext(entries),
              )
            )

          case LedgerEffects =>
            Option.when(events.nonEmpty)(
              toApiTransaction(
                first = first,
                events = events,
                traceContext = extractTraceContext(entries),
              )
            )
        }
      }

    private def toApiTransaction(
        first: Entry[Event],
        events: Seq[Event],
        traceContext: Option[DamlTraceContext],
    ): ApiTransaction =
      ApiTransaction(
        updateId = first.updateId,
        commandId = first.commandId.getOrElse(""),
        effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
        workflowId = first.workflowId.getOrElse(""),
        offset = first.offset,
        events = events,
        synchronizerId = first.synchronizerId,
        traceContext = traceContext,
        recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
        externalTransactionHash = first.externalTransactionHash.map(ByteString.copyFrom),
      )

    def toGetTransactionsResponse(
        events: Seq[Entry[Event]],
        transactionShape: TransactionShape,
    ): List[(Long, GetUpdatesResponse)] =
      toTransaction(events, transactionShape).toList.map(tx =>
        tx.offset -> GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(tx))
          .withPrecomputedSerializedSize()
      )

    def toParticipantPermission(level: AuthorizationLevel): ParticipantPermission = level match {
      case Submission => PARTICIPANT_PERMISSION_SUBMISSION
      case Confirmation => PARTICIPANT_PERMISSION_CONFIRMATION
      case Observation => PARTICIPANT_PERMISSION_OBSERVATION
    }

    def toTopologyEvent(
        partyId: String,
        participantId: String,
        authorizationEvent: AuthorizationEvent,
    ): TopologyEvent =
      TopologyEvent {
        authorizationEvent match {
          case AuthorizationEvent.Added(level) =>
            TopologyEvent.Event.ParticipantAuthorizationAdded(
              ParticipantAuthorizationAdded(
                partyId = partyId,
                participantId = participantId,
                participantPermission = toParticipantPermission(level),
              )
            )
          case AuthorizationEvent.ChangedTo(level) =>
            TopologyEvent.Event.ParticipantAuthorizationChanged(
              ParticipantAuthorizationChanged(
                partyId = partyId,
                participantId = participantId,
                participantPermission = toParticipantPermission(level),
              )
            )
          case AuthorizationEvent.Revoked =>
            TopologyEvent.Event.ParticipantAuthorizationRevoked(
              ParticipantAuthorizationRevoked(
                partyId = partyId,
                participantId = participantId,
              )
            )
        }
      }

    def toTopologyTransaction(
        events: Vector[RawParticipantAuthorization]
    ): Option[(Offset, TopologyTransaction)] =
      events.headOption.map { first =>
        first.offset ->
          TopologyTransaction(
            updateId = first.updateId,
            events = events
              .map(event =>
                toTopologyEvent(
                  partyId = event.partyId,
                  participantId = event.participantId,
                  authorizationEvent = event.authorizationEvent,
                )
              ),
            offset = first.offset.unwrap,
            synchronizerId = first.synchronizerId,
            traceContext = first.traceContext.map(DamlTraceContext.parseFrom),
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
          )
      }

    // TODO(#23504) remove when TreeEvent is removed
    @nowarn("cat=deprecation")
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

    // TODO(#23504) remove when TreeEvent is removed
    @nowarn("cat=deprecation")
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

    // TODO(#23504) remove when TreeEvent is removed
    @nowarn("cat=deprecation")
    def toGetTransactionTreesResponse(
        events: Seq[Entry[TreeEvent]]
    ): List[(Long, GetUpdateTreesResponse)] =
      transactionTree(events).toList.map(tx =>
        tx.offset -> GetUpdateTreesResponse(GetUpdateTreesResponse.Update.TransactionTree(tx))
          .withPrecomputedSerializedSize()
      )

    // TODO(#23504) remove when TreeEvent is removed
    @nowarn("cat=deprecation")
    def toGetTransactionTreeResponse(
        events: Seq[Entry[TreeEvent]]
    ): Option[GetTransactionTreeResponse] =
      transactionTree(events).map(tx => GetTransactionTreeResponse(Some(tx)))

  }

}
