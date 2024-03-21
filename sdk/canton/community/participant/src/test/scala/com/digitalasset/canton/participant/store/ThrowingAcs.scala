// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  AcsError,
  AcsWarning,
  ContractState,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.{LfContractId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT}

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

class ThrowingAcs[T <: Throwable](mk: String => T)(override implicit val ec: ExecutionContext)
    extends ActiveContractStore {
  private[this] type M = Checked[AcsError, AcsWarning, Unit]

  override def markContractsActive(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"createContracts for $contracts at $toc")))

  override def archiveContracts(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"archiveContracts for $contracts at $toc")))

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"transferInContracts for $transferIns")))

  override def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.failed[M](mk(s"transferOutContracts for $transferOuts")))

  override def fetchStates(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ContractState]] =
    Future.failed(mk(s"fetchContractStates for $contractIds"))

  /** Always returns [[scala.Map$.empty]] so that the failure does not happen while checking the invariant. */
  override def fetchStatesForInvariantChecking(ids: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, StateChange[ActiveContractStore.Status]]] =
    Future.successful(Map.empty)

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, CantonTimestamp]] =
    Future.failed(mk(s"snapshot at $timestamp"))

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, RequestCounter]] =
    Future.failed(mk(s"snapshot at $rc"))

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] =
    Future.failed[Map[LfContractId, CantonTimestamp]](
      mk(s"contractSnapshot for $contractIds at $timestamp")
    )

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.failed(mk(s"doPrune at $beforeAndIncluding"))

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.failed(mk(s"advancePruningTimestamp"))

  /** Always returns [[scala.None$]] so that the failure does not happen while checking the invariant. */
  override def pruningStatus(implicit
      traceContext: TraceContext
  ): Future[Option[PruningStatus]] =
    Future.successful(None)

  override def deleteSince(criterion: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.failed[Unit](mk(s"deleteSince at $criterion"))

  override def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.failed(mk(s"contractCount at $timestamp"))

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    Future.failed(mk(s"changesBetween for $fromExclusive, $toInclusive"))

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[(LfContractId)]] =
    Future.failed(mk(s"packageUnused for $pkg"))
}
