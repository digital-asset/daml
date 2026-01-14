// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.CantonRequireTypes.String36
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.participant.store.db.{
  DbParticipantPruningStore,
  DbParticipantPruningStoreCached,
}
import com.digitalasset.canton.participant.store.memory.InMemoryParticipantPruningStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** The ParticipantPruningStore stores the last started / completed pruning operation.
  */
trait ParticipantPruningStore extends AutoCloseable {

  protected implicit def ec: ExecutionContext

  def markPruningStarted(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def markPruningDone(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def pruningStatus()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ParticipantPruningStatus]
}

object ParticipantPruningStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[ParticipantPruningStore] =
    storage match {
      case _: MemoryStorage =>
        FutureUnlessShutdown.pure(new InMemoryParticipantPruningStore(loggerFactory))
      case dbStorage: DbStorage =>
        val dbStore = new DbParticipantPruningStore(dbStoreName, dbStorage, timeouts, loggerFactory)
        dbStore.pruningStatus().map { initialStatus =>
          new DbParticipantPruningStoreCached(
            underlying = dbStore,
            initialStatus = initialStatus,
            loggerFactory = loggerFactory,
          )
        }

    }

  private val dbStoreName = String36.tryCreate("DbParticipantPruningStore")

  final case class ParticipantPruningStatus(
      startedO: Option[Offset],
      completedO: Option[Offset],
  ) extends PrettyPrinting {
    def isInProgress: Boolean =
      startedO.exists(started => completedO.forall(completed => started > completed))

    override protected def pretty: Pretty[ParticipantPruningStatus] =
      prettyOfClass(
        paramIfDefined("started", _.startedO),
        paramIfDefined("completed", _.completedO),
      )
  }
}
