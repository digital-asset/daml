// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.store.ParticipantPruningStore
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InMemoryParticipantPruningStore(protected val loggerFactory: NamedLoggerFactory)(implicit
    val ec: ExecutionContext
) extends ParticipantPruningStore {

  private val status: AtomicReference[ParticipantPruningStatus] =
    new AtomicReference(ParticipantPruningStatus(None, None))

  override def markPruningStarted(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {
    status.updateAndGet {
      case oldStatus if oldStatus.startedO.forall(_ < upToInclusive) =>
        oldStatus.copy(startedO = Some(upToInclusive))
      case oldStatus => oldStatus
    }
    Future.unit
  }

  override def markPruningDone(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {
    status.updateAndGet {
      case oldStatus if oldStatus.completedO.forall(_ < upToInclusive) =>
        oldStatus.copy(completedO = Some(upToInclusive))
      case oldStatus => oldStatus
    }
    Future.unit
  }

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): Future[ParticipantPruningStatus] =
    Future.successful(status.get())

  override def close(): Unit = ()
}
