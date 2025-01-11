// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  AcsError,
  AcsWarning,
  ActivenessChangeDetail,
  ContractState,
}
import com.digitalasset.canton.participant.store.HookedAcs.noFetchAction
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{CheckedT, ReassignmentTag}
import com.digitalasset.canton.{ReassignmentCounter, RequestCounter}
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

private[participant] class HookedAcs(private val acs: ActiveContractStore)(implicit
    val ec: ExecutionContext
) extends ActiveContractStore {
  import HookedAcs.{noArchivePurgeAction, noCreateAddAction, noReassignmentAction}

  private val nextCreateAddHook: AtomicReference[
    (Seq[(LfContractId, ReassignmentCounter, TimeOfChange)]) => FutureUnlessShutdown[Unit]
  ] =
    new AtomicReference[
      (Seq[(LfContractId, ReassignmentCounter, TimeOfChange)]) => FutureUnlessShutdown[Unit]
    ](
      noCreateAddAction
    )
  private val nextArchivePurgeHook
      : AtomicReference[Seq[(LfContractId, TimeOfChange)] => FutureUnlessShutdown[Unit]] =
    new AtomicReference[Seq[(LfContractId, TimeOfChange)] => FutureUnlessShutdown[Unit]](
      noArchivePurgeAction
    )

  private val nextReassignmentHook =
    new AtomicReference[
      (
          Seq[(LfContractId, ReassignmentTag[SynchronizerId], ReassignmentCounter, TimeOfChange)],
          Boolean, // true for unassignments, false for assignments
      ) => FutureUnlessShutdown[Unit]
    ](
      noReassignmentAction
    )
  private val nextFetchHook: AtomicReference[Iterable[LfContractId] => FutureUnlessShutdown[Unit]] =
    new AtomicReference[Iterable[LfContractId] => FutureUnlessShutdown[Unit]](noFetchAction)

  override private[store] def indexedStringStore: IndexedStringStore = acs.indexedStringStore

  def setCreateAddHook(
      preCreate: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)] => FutureUnlessShutdown[
        Unit
      ]
  ): Unit =
    nextCreateAddHook.set(preCreate)

  def setArchivePurgeHook(
      preArchive: Seq[(LfContractId, TimeOfChange)] => FutureUnlessShutdown[Unit]
  ): Unit =
    nextArchivePurgeHook.set(preArchive)

  def setReassignmentHook(
      preReassignment: (
          Seq[(LfContractId, ReassignmentTag[SynchronizerId], ReassignmentCounter, TimeOfChange)],
          Boolean,
      ) => FutureUnlessShutdown[Unit]
  ): Unit =
    nextReassignmentHook.set(preReassignment)

  def setFetchHook(preFetch: Iterable[LfContractId] => FutureUnlessShutdown[Unit]): Unit =
    nextFetchHook.set(preFetch)

  override def markContractsCreatedOrAdded(
      contracts: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)],
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = CheckedT {
    val preCreate = nextCreateAddHook.getAndSet(noCreateAddAction)
    preCreate(contracts).flatMap { _ =>
      acs.markContractsCreatedOrAdded(contracts, isCreation).value
    }
  }

  override def purgeOrArchiveContracts(
      contracts: Seq[(LfContractId, TimeOfChange)],
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = CheckedT {
    val preArchive = nextArchivePurgeHook.getAndSet(noArchivePurgeAction)
    preArchive(contracts)
      .flatMap { _ =>
        acs.purgeOrArchiveContracts(contracts, isArchival).value
      }
  }

  override def assignContracts(
      assignments: Seq[(LfContractId, Source[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = CheckedT {
    val preReassignment = nextReassignmentHook.getAndSet(noReassignmentAction)
    preReassignment(assignments, false).flatMap { _ =>
      acs.assignContracts(assignments).value
    }
  }

  override def unassignContracts(
      unassignments: Seq[(LfContractId, Target[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = CheckedT {
    val preReassignment = nextReassignmentHook.getAndSet(noReassignmentAction)
    preReassignment(
      unassignments,
      true,
    )
      .flatMap { _ =>
        acs.unassignContracts(unassignments).value
      }
  }

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfContractId, ContractState]] = {
    val preFetch = nextFetchHook.getAndSet(noFetchAction)
    preFetch(contractIds)
      .flatMap(_ => acs.fetchStates(contractIds))
  }

  override def fetchStatesForInvariantChecking(ids: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, StateChange[ActiveContractStore.Status]]] =
    acs.fetchStatesForInvariantChecking(ids)

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]] =
    acs.snapshot(timestamp)

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, (RequestCounter, ReassignmentCounter)]] =
    acs.snapshot(rc)

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, CantonTimestamp]] =
    acs.contractSnapshot(contractIds, timestamp)

  override def bulkContractsReassignmentCounterSnapshot(
      contractIds: Set[LfContractId],
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ReassignmentCounter]] =
    acs.bulkContractsReassignmentCounterSnapshot(contractIds, requestCounter)

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    acs.doPrune(beforeAndIncluding, lastPruning: Option[CantonTimestamp])

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    acs.purge()

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    acs.advancePruningTimestamp(phase, timestamp)

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PruningStatus]] =
    acs.pruningStatus

  override def deleteSince(criterion: RequestCounter)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    acs.deleteSince(criterion)

  override def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    acs.contractCount(timestamp)

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    acs.changesBetween(fromExclusive, toInclusive)

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[LfContractId]] = acs.packageUsage(pkg, contractStore)

  override def activenessOf(contracts: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]
  ] =
    acs.activenessOf(contracts)
}

object HookedAcs {
  private val noCreateAddAction
      : Seq[(LfContractId, ReassignmentCounter, TimeOfChange)] => FutureUnlessShutdown[Unit] = _ =>
    FutureUnlessShutdown.unit

  private val noArchivePurgeAction
      : Seq[(LfContractId, TimeOfChange)] => FutureUnlessShutdown[Unit] = _ =>
    FutureUnlessShutdown.unit

  private val noReassignmentAction: (
      Seq[(LfContractId, ReassignmentTag[SynchronizerId], ReassignmentCounter, TimeOfChange)],
      Boolean,
  ) => FutureUnlessShutdown[Unit] = { (_, _) => FutureUnlessShutdown.unit }

  private val noFetchAction: Iterable[LfContractId] => FutureUnlessShutdown[Unit] = _ =>
    FutureUnlessShutdown.unit
}
