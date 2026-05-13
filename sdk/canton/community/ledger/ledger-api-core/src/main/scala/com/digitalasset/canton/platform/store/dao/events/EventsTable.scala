// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

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
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawParticipantAuthorization

object EventsTable {

  object TransactionConversions {
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
