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
import com.daml.ledger.api.v2.transaction.Transaction as ApiTransaction
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
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
import com.google.protobuf.ByteString

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
        effectiveAt = Some(
          first.event.event.created
            .flatMap(_.createdAt)
            .orElse(first.ledgerEffectiveTime.map(TimestampConversion.fromLf))
            .getOrElse(
              throw new IllegalStateException(
                "Either newly created contracts or the Entry for exercises should provide the ledgerEffectiveTime."
              )
            )
        ),
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
  }

}
