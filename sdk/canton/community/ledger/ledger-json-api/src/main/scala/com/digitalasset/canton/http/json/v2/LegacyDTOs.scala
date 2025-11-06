// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.{offset_checkpoint, reassignment}

/** Data structures that replicate legacy gRPC messages for backwards compatibility */
// TODO(#27734) remove when json legacy endpoints are removed
object LegacyDTOs {
  final case class TreeEvent(
      kind: TreeEvent.Kind
  )

  object TreeEvent {
    sealed trait Kind
    object Kind {
      case object Empty extends Kind
      final case class Created(value: com.daml.ledger.api.v2.event.CreatedEvent) extends Kind
      final case class Exercised(value: com.daml.ledger.api.v2.event.ExercisedEvent) extends Kind
    }
  }

  final case class TransactionTree(
      updateId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Option[com.google.protobuf.timestamp.Timestamp],
      offset: Long,
      eventsById: collection.immutable.Map[
        Int,
        TreeEvent,
      ],
      synchronizerId: String,
      traceContext: Option[com.daml.ledger.api.v2.trace_context.TraceContext],
      recordTime: Option[com.google.protobuf.timestamp.Timestamp],
  )

  final case class GetUpdateTreesResponse(
      update: GetUpdateTreesResponse.Update
  )

  object GetUpdateTreesResponse {
    sealed trait Update
    object Update {
      final case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
      final case class Reassignment(value: reassignment.Reassignment) extends Update
      final case class TransactionTree(value: LegacyDTOs.TransactionTree) extends Update
      final case object Empty extends Update
    }
  }

  final case class TransactionFilter(
      filtersByParty: collection.immutable.Map[
        String,
        com.daml.ledger.api.v2.transaction_filter.Filters,
      ],
      filtersForAnyParty: Option[com.daml.ledger.api.v2.transaction_filter.Filters],
  )

  final case class GetUpdatesRequest(
      beginExclusive: Long,
      endInclusive: Option[Long],
      filter: Option[LegacyDTOs.TransactionFilter],
      verbose: Boolean = false,
      updateFormat: Option[com.daml.ledger.api.v2.transaction_filter.UpdateFormat],
  )

  final case class GetActiveContractsRequest(
      filter: Option[LegacyDTOs.TransactionFilter],
      verbose: Boolean = false,
      activeAtOffset: Long,
      eventFormat: Option[com.daml.ledger.api.v2.transaction_filter.EventFormat],
  )

  final case class SubmitAndWaitForTransactionTreeResponse(
      transaction: Option[TransactionTree]
  )

  def toTransactionTree(tx: Transaction): LegacyDTOs.TransactionTree =
    LegacyDTOs.TransactionTree(
      updateId = tx.updateId,
      commandId = tx.commandId,
      workflowId = tx.workflowId,
      effectiveAt = tx.effectiveAt,
      offset = tx.offset,
      eventsById = tx.events
        .collect(e =>
          e.event match {
            case Event.Created(created) =>
              created.nodeId -> LegacyDTOs.TreeEvent(LegacyDTOs.TreeEvent.Kind.Created(created))
            case Event.Exercised(exercised) =>
              exercised.nodeId -> LegacyDTOs.TreeEvent(
                LegacyDTOs.TreeEvent.Kind.Exercised(exercised)
              )
          }
        )
        .toMap,
      synchronizerId = tx.synchronizerId,
      traceContext = tx.traceContext,
      recordTime = tx.recordTime,
    )

  final case class GetTransactionTreeResponse(
      transaction: Option[TransactionTree]
  )

  final case class GetTransactionByIdRequest(
      updateId: String,
      requestingParties: Seq[String],
      transactionFormat: Option[com.daml.ledger.api.v2.transaction_filter.TransactionFormat],
  )

  final case class GetTransactionByOffsetRequest(
      offset: Long,
      requestingParties: Seq[String],
      transactionFormat: Option[com.daml.ledger.api.v2.transaction_filter.TransactionFormat],
  )

  final case class GetTransactionResponse(
      transaction: Option[com.daml.ledger.api.v2.transaction.Transaction]
  )

}
