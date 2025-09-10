// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

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
      verbose: Boolean,
      updateFormat: Option[com.daml.ledger.api.v2.transaction_filter.UpdateFormat],
  )

  final case class GetActiveContractsRequest(
      filter: Option[LegacyDTOs.TransactionFilter],
      verbose: Boolean,
      activeAtOffset: Long,
      eventFormat: Option[com.daml.ledger.api.v2.transaction_filter.EventFormat],
  )

}
