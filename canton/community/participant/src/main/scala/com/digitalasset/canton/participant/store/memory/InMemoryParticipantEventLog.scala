// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.store.EventLogId.ParticipantEventLogId
import com.digitalasset.canton.participant.store.ParticipantEventLog
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InMemoryParticipantEventLog(id: ParticipantEventLogId, loggerFactory: NamedLoggerFactory)(
    implicit ec: ExecutionContext
) extends InMemorySingleDimensionEventLog[ParticipantEventLogId](id, loggerFactory)
    with ParticipantEventLog {

  private val nextRequestCounterRef =
    new AtomicReference[RequestCounter](InMemoryParticipantEventLog.InitialCounter)

  override def nextLocalOffsets(
      count: NonNegativeInt
  )(implicit traceContext: TraceContext): Future[Seq[LocalOffset]] =
    Future.successful {
      val oldCounter = nextRequestCounterRef.getAndUpdate(offset => offset + count.unwrap.toLong)

      oldCounter
        .until(oldCounter + count.unwrap.toLong)
        .map(LocalOffset(_))
    }

  override def firstEventWithAssociatedDomainAtOrAfter(
      associatedDomain: DomainId,
      atOrAfter: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[TimestampedEvent]] =
    Future.successful {
      state.get().eventsByOffset.collectFirst {
        case (_localOffset, event)
            if event.eventId.exists(
              _.associatedDomain.contains(associatedDomain)
            ) && event.timestamp >= atOrAfter =>
          event
      }
    }

  override def close(): Unit = ()
}

object InMemoryParticipantEventLog {
  private val InitialCounter: RequestCounter = RequestCounter.Genesis
}
