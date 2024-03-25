// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.CantonRequireTypes.String36
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.participant.store.db.DbParticipantPruningStore
import com.digitalasset.canton.participant.store.memory.InMemoryParticipantPruningStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** The ParticipantPruningStore stores the last started / completed pruning operation.
  */
trait ParticipantPruningStore extends AutoCloseable {

  protected implicit def ec: ExecutionContext

  def markPruningStarted(upToInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def markPruningDone(upToInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def pruningStatus()(implicit traceContext: TraceContext): Future[ParticipantPruningStatus]
}

object ParticipantPruningStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): ParticipantPruningStore =
    storage match {
      case _: MemoryStorage => new InMemoryParticipantPruningStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbParticipantPruningStore(dbStoreName, dbStorage, timeouts, loggerFactory)
    }

  private val dbStoreName = String36.tryCreate("DbParticipantPruningStore")

  final case class ParticipantPruningStatus(
      startedO: Option[GlobalOffset],
      completedO: Option[GlobalOffset],
  ) extends PrettyPrinting {
    def isInProgress: Boolean =
      startedO.exists(started => completedO.forall(completed => started > completed))

    override def pretty: Pretty[ParticipantPruningStatus] =
      prettyOfClass(
        paramIfDefined("started", _.startedO),
        paramIfDefined("completed", _.completedO),
      )
  }
}
