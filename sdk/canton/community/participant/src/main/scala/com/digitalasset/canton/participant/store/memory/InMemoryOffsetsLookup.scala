// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateLookup
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait InMemoryOffsetsLookup {
  def lookupOffsetsBetween(
      id: EventLogId
  )(fromExclusive: Option[LocalOffset], upToInclusive: Option[LocalOffset])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Seq[LocalOffset]]
}

class InMemoryOffsetsLookupImpl(
    syncDomainPersistentStates: SyncDomainPersistentStateLookup,
    participantEventLog: ParticipantEventLog,
) extends InMemoryOffsetsLookup {

  def lookupOffsetsBetween(
      id: EventLogId
  )(fromExclusive: Option[LocalOffset], upToInclusive: Option[LocalOffset])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Seq[LocalOffset]] = {
    for {
      events <- allEventLogs(id).lookupEventRange(
        fromExclusive,
        upToInclusive,
        None,
        None,
        None,
      )
    } yield events.keySet.toSeq
  }

  private def allEventLogs: Map[EventLogId, SingleDimensionEventLog[EventLogId]] =
    syncDomainPersistentStates.getAll.map { case (_, state) =>
      val eventLog = state.eventLog
      (eventLog.id: EventLogId) -> eventLog
    } + (participantEventLog.id -> participantEventLog)
}
