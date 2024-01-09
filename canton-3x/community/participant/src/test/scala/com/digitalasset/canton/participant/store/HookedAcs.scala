// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  AcsError,
  AcsWarning,
  ContractState,
}
import com.digitalasset.canton.participant.store.HookedAcs.noFetchAction
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.{
  LfContractId,
  SourceDomainId,
  TargetDomainId,
  TransferDomainId,
}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.CheckedT
import com.digitalasset.canton.{RequestCounter, TransferCounterO}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

private[participant] class HookedAcs(private val acs: ActiveContractStore)(implicit
    val ec: ExecutionContext
) extends ActiveContractStore {
  import HookedAcs.{noArchiveAction, noCreateAction, noTransferAction}

  private val nextCreateHook
      : AtomicReference[(Seq[(LfContractId, TransferCounterO)], TimeOfChange) => Future[Unit]] =
    new AtomicReference[(Seq[(LfContractId, TransferCounterO)], TimeOfChange) => Future[Unit]](
      noCreateAction
    )
  private val nextArchiveHook: AtomicReference[(Seq[LfContractId], TimeOfChange) => Future[Unit]] =
    new AtomicReference[(Seq[LfContractId], TimeOfChange) => Future[Unit]](noArchiveAction)
  private val nextTransferHook =
    new AtomicReference[
      (
          Seq[(LfContractId, TransferDomainId, TransferCounterO, TimeOfChange)],
          Boolean,
      ) => Future[Unit]
    ](
      noTransferAction
    )
  private val nextFetchHook: AtomicReference[Iterable[LfContractId] => Future[Unit]] =
    new AtomicReference[Iterable[LfContractId] => Future[Unit]](noFetchAction)

  def setCreateHook(
      preCreate: (Seq[(LfContractId, TransferCounterO)], TimeOfChange) => Future[Unit]
  ): Unit =
    nextCreateHook.set(preCreate)
  def setArchiveHook(preArchive: (Seq[LfContractId], TimeOfChange) => Future[Unit]): Unit =
    nextArchiveHook.set(preArchive)
  def setTransferHook(
      preTransfer: (
          Seq[(LfContractId, TransferDomainId, TransferCounterO, TimeOfChange)],
          Boolean,
      ) => Future[Unit]
  ): Unit =
    nextTransferHook.set(preTransfer)
  def setFetchHook(preFetch: Iterable[LfContractId] => Future[Unit]): Unit =
    nextFetchHook.set(preFetch)

  override def markContractsActive(
      contracts: Seq[(LfContractId, TransferCounterO)],
      toc: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preCreate = nextCreateHook.getAndSet(noCreateAction)
    preCreate(contracts, toc).flatMap { _ =>
      acs.markContractsActive(contracts, toc).value
    }
  }

  override def archiveContracts(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preArchive = nextArchiveHook.getAndSet(noArchiveAction)
    preArchive(contracts, toc)
      .flatMap { _ =>
        acs.archiveContracts(contracts, toc).value
      }
  }

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TransferCounterO, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preTransfer = nextTransferHook.getAndSet(noTransferAction)
    preTransfer(
      transferIns,
      false,
    ).flatMap { _ =>
      acs.transferInContracts(transferIns).value
    }
  }

  override def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TransferCounterO, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = CheckedT {
    val preTransfer = nextTransferHook.getAndSet(noTransferAction)
    preTransfer(transferOuts, true).flatMap { _ =>
      acs.transferOutContracts(transferOuts).value
    }
  }

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] = {
    val preFetch = nextFetchHook.getAndSet(noFetchAction)
    preFetch(contractIds).flatMap(_ => acs.fetchStates(contractIds))
  }

  override def fetchStatesForInvariantChecking(ids: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, StateChange[ActiveContractStore.Status]]] =
    acs.fetchStatesForInvariantChecking(ids)

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (CantonTimestamp, TransferCounterO)]] =
    acs.snapshot(timestamp)

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (RequestCounter, TransferCounterO)]] =
    acs.snapshot(rc)

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] =
    acs.contractSnapshot(contractIds, timestamp)

  override def bulkContractsTransferCounterSnapshot(
      contractIds: Set[LfContractId],
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, TransferCounterO]] =
    acs.bulkContractsTransferCounterSnapshot(contractIds, requestCounter)

  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int] =
    acs.doPrune(beforeAndIncluding, lastPruning: Option[CantonTimestamp])

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    acs.advancePruningTimestamp(phase, timestamp)

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): Future[Option[PruningStatus]] =
    acs.pruningStatus

  override def deleteSince(criterion: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    acs.deleteSince(criterion)

  override def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    acs.contractCount(timestamp)

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    acs.changesBetween(fromExclusive, toInclusive)

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[LfContractId]] = acs.packageUsage(pkg, contractStore)
}

object HookedAcs {
  private val noCreateAction
      : (Seq[(LfContractId, TransferCounterO)], TimeOfChange) => Future[Unit] = { (_, _) =>
    Future.unit
  }

  private val noArchiveAction: (Seq[LfContractId], TimeOfChange) => Future[Unit] = { (_, _) =>
    Future.unit
  }

  private val noTransferAction: (
      Seq[(LfContractId, TransferDomainId, TransferCounterO, TimeOfChange)],
      Boolean,
  ) => Future[Unit] = { (_, _) =>
    Future.unit
  }

  private val noFetchAction: Iterable[LfContractId] => Future[Unit] = _ => Future.unit
}
