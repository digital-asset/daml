// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.ContractKeyJournal.{
  ContractKeyJournalError,
  ContractKeyState,
  Status,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfGlobalKey
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class PreUpdateHookCkj(private val ckj: ContractKeyJournal)(
    override implicit val ec: ExecutionContext
) extends ContractKeyJournal {
  import PreUpdateHookCkj.*

  private val nextAddKeyStateUpdatedHook =
    new AtomicReference[AddKeyStateUpdateHook](noKeyStateUpdateHook)

  def setUpdateHook(newHook: AddKeyStateUpdateHook): Unit = nextAddKeyStateUpdatedHook.set(newHook)

  override def fetchStates(keys: Iterable[LfGlobalKey])(implicit
      traceContext: TraceContext
  ): Future[Map[LfGlobalKey, ContractKeyState]] =
    ckj.fetchStates(keys)

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, (Status, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] = {
    val preUpdate = nextAddKeyStateUpdatedHook.getAndSet(noKeyStateUpdateHook)
    preUpdate(updates).flatMap(_ => ckj.addKeyStateUpdates(updates))
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int] =
    ckj.doPrune(beforeAndIncluding, lastPruning)

  override def deleteSince(inclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    ckj.deleteSince(inclusive)

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    ckj.countUpdates(key)

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): Future[Option[PruningStatus]] =
    ckj.pruningStatus

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    ckj.advancePruningTimestamp(phase, timestamp)
}

object PreUpdateHookCkj {
  type AddKeyStateUpdateHook =
    (Map[LfGlobalKey, (Status, TimeOfChange)]) => EitherT[Future, ContractKeyJournalError, Unit]

  val noKeyStateUpdateHook: AddKeyStateUpdateHook = _ =>
    EitherT(Future.successful(Either.right(())))
}
