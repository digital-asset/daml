// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.checked
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.EventLogId.ParticipantEventLogId
import com.digitalasset.canton.participant.store.db.DbParticipantEventLog
import com.digitalasset.canton.participant.store.memory.InMemoryParticipantEventLog
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.{LocalOffset, RequestOffset}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait ParticipantEventLog
    extends SingleDimensionEventLog[ParticipantEventLogId]
    with AutoCloseable {
  this: NamedLogging =>

  /** Returns the first event (by offset ordering) whose [[com.digitalasset.canton.participant.sync.TimestampedEvent.EventId]]
    * has the given `associatedDomain` and whose timestamp is at or after `atOrAfter`.
    *
    * @throws java.lang.UnsupportedOperationException if this event log is not the participant event log.
    */
  def firstEventWithAssociatedDomainAtOrAfter(
      associatedDomain: DomainId,
      atOrAfter: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[TimestampedEvent]]

  /** Allocates the next local offset */
  def nextLocalOffset()(implicit traceContext: TraceContext): Future[LocalOffset] =
    nextLocalOffsets(NonNegativeInt.one).map(
      _.headOption.getOrElse(
        ErrorUtil.internalError(
          new RuntimeException("failed to allocate at least one local offset")
        )
      )
    )

  /** Allocates `count` many new offsets and returns all of them.
    */
  def nextLocalOffsets(count: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Seq[RequestOffset]]
}

object ParticipantEventLog {

  /** There is no meaningful `effectiveTime` for the RequestOffset: since the participant event log contains
    *  data related to several domains as well as participant local events, there are several incomparable
    *  clocks in scope. Thus, we consider all `effectiveTime` to be the same.
    */
  private[store] val EffectiveTime: CantonTimestamp = CantonTimestamp.Epoch

  val ProductionParticipantEventLogId: ParticipantEventLogId = checked(
    ParticipantEventLogId.tryCreate(0)
  )

  def apply(
      storage: Storage,
      indexedStringStore: IndexedStringStore,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ParticipantEventLog =
    storage match {
      case _: MemoryStorage =>
        new InMemoryParticipantEventLog(ProductionParticipantEventLogId, loggerFactory)
      case dbStorage: DbStorage =>
        new DbParticipantEventLog(
          ProductionParticipantEventLogId,
          dbStorage,
          indexedStringStore,
          releaseProtocolVersion,
          timeouts,
          loggerFactory,
        )
    }
}
