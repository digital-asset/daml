// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  AcsError,
  AcsWarning,
  ActivenessChangeDetail,
  ContractState,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

class ThrowingAcs[T <: Throwable](mk: String => T)(override implicit val ec: ExecutionContext)
    extends ActiveContractStore {
  private[this] type M = Checked[AcsError, AcsWarning, Unit]

  override private[store] def indexedStringStore: IndexedStringStore = throw new RuntimeException(
    "I should not be called"
  )

  override def markContractsCreatedOrAdded(
      contracts: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)],
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val operation = if (isCreation) "create contracts" else "add contracts"
    CheckedT(FutureUnlessShutdown.failed[M](mk(s"$operation for $contracts")))
  }

  override def purgeOrArchiveContracts(
      contracts: Seq[(LfContractId, TimeOfChange)],
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val operation = if (isArchival) "archive contracts" else "purge contracts"
    CheckedT(FutureUnlessShutdown.failed[M](mk(s"$operation for $contracts")))
  }

  override def assignContracts(
      assignments: Seq[(LfContractId, Source[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    CheckedT(FutureUnlessShutdown.failed[M](mk(s"assignContracts for $assignments")))

  override def unassignContracts(
      unassignments: Seq[(LfContractId, Target[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    CheckedT(FutureUnlessShutdown.failed[M](mk(s"unassignContracts for $unassignments")))

  override def fetchStates(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ContractState]] =
    FutureUnlessShutdown.failed(mk(s"fetchContractStates for $contractIds"))

  /** Always returns [[scala.Map$.empty]] so that the failure does not happen while checking the
    * invariant.
    */
  override def fetchStatesForInvariantChecking(ids: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, StateChange[ActiveContractStore.Status]]] =
    FutureUnlessShutdown.pure(Map.empty)

  override def snapshot(toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, (TimeOfChange, ReassignmentCounter)]] =
    FutureUnlessShutdown.failed(mk(s"snapshot at $toc"))

  override def contractSnapshot(contractIds: Set[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, TimeOfChange]] =
    FutureUnlessShutdown.failed[Map[LfContractId, TimeOfChange]](
      mk(s"contractSnapshot for $contractIds at $toc")
    )

  override def contractsReassignmentCounterSnapshotBefore(
      contractIds: Set[LfContractId],
      timestampExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ReassignmentCounter]] =
    FutureUnlessShutdown.failed[Map[LfContractId, ReassignmentCounter]](
      mk(
        s"bulkContractsReassignmentCounterSnapshot for $contractIds up to but not including $timestampExclusive"
      )
    )

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.failed(mk(s"doPrune at $beforeAndIncluding"))

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.failed(mk("purge"))

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.failed(mk(s"advancePruningTimestamp"))

  /** Always returns [[scala.None$]] so that the failure does not happen while checking the
    * invariant.
    */
  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PruningStatus]] =
    FutureUnlessShutdown.pure(None)

  override def deleteSince(criterion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.failed[Unit](mk(s"deleteSince at $criterion"))

  override def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.failed(mk(s"contractCount at $timestamp"))

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    FutureUnlessShutdown.failed(mk(s"changesBetween for $fromExclusive, $toInclusive"))

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[LfContractId]] =
    FutureUnlessShutdown.failed(mk(s"packageUnused for $pkg"))

  override def activenessOf(contracts: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]
  ] =
    FutureUnlessShutdown.failed(mk(s"activenessOf for $contracts"))
}
