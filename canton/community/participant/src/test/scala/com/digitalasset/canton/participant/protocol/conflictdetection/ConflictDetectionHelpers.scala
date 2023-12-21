// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  TransferredAway,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryActiveContractStore,
  InMemoryContractKeyJournal,
  InMemoryTransferStore,
  TransferCache,
}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractKeyJournal,
  TransferStore,
  TransferStoreTest,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DomainId, MediatorId, MediatorRef}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LfPartyId,
  ScalaFuturesWithPatience,
  TransferCounter,
  TransferCounterO,
}
import org.scalactic.source.Position
import org.scalatest.AsyncTestSuite
import org.scalatest.exceptions.TestFailedException

import scala.concurrent.{ExecutionContext, Future}

private[protocol] trait ConflictDetectionHelpers {
  this: AsyncTestSuite with BaseTest with HasExecutorService =>

  import ConflictDetectionHelpers.*

  def parallelExecutionContext: ExecutionContext = executorService

  def mkEmptyAcs(): ActiveContractStore =
    new InMemoryActiveContractStore(testedProtocolVersion, loggerFactory)(
      parallelExecutionContext
    )

  def mkAcs(
      entries: (LfContractId, TimeOfChange, ActiveContractStore.Status)*
  )(implicit traceContext: TraceContext): Future[ActiveContractStore] = {
    val acs = mkEmptyAcs()
    insertEntriesAcs(acs, entries).map(_ => acs)
  }

  def mkEmptyCkj(): ContractKeyJournal = new InMemoryContractKeyJournal(loggerFactory)(
    parallelExecutionContext
  )

  def mkCkj(
      entries: (LfGlobalKey, TimeOfChange, ContractKeyJournal.Status)*
  )(implicit traceContext: TraceContext, position: Position): Future[ContractKeyJournal] = {
    val ckj = mkEmptyCkj()
    insertEntriesCkj(ckj, entries).map(_ => ckj)
  }

  def mkTransferCache(
      loggerFactory: NamedLoggerFactory,
      store: TransferStore =
        new InMemoryTransferStore(TransferStoreTest.targetDomain, loggerFactory),
  )(
      entries: (TransferId, MediatorId)*
  )(implicit traceContext: TraceContext): Future[TransferCache] = {
    Future
      .traverse(entries) { case (transferId, sourceMediator) =>
        for {
          transfer <- TransferStoreTest.mkTransferDataForDomain(
            transferId,
            MediatorRef(sourceMediator),
            targetDomainId = TransferStoreTest.targetDomain,
          )
          result <- store
            .addTransfer(transfer)
            .value
        } yield result
      }
      .map(_ => new TransferCache(store, loggerFactory)(parallelExecutionContext))
  }
}

private[protocol] object ConflictDetectionHelpers extends ScalaFuturesWithPatience {

  private val initialTransferCounter: TransferCounterO =
    Some(TransferCounter.Genesis)

  def insertEntriesAcs(
      acs: ActiveContractStore,
      entries: Seq[(LfContractId, TimeOfChange, ActiveContractStore.Status)],
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    Future
      .traverse(entries) {
        case (coid, toc, Active(_transferCounter)) =>
          acs
            .markContractActive(coid -> initialTransferCounter, toc)
            .value
        case (coid, toc, Archived) =>
          acs.archiveContract(coid, toc).value
        case (coid, toc, TransferredAway(targetDomain, transferCounter)) =>
          acs.transferOutContract(coid, toc, targetDomain, transferCounter).value
      }
      .void
  }

  def insertEntriesCkj(
      ckj: ContractKeyJournal,
      entries: Seq[(LfGlobalKey, TimeOfChange, ContractKeyJournal.Status)],
  )(implicit ec: ExecutionContext, traceContext: TraceContext, position: Position): Future[Unit] = {
    Future
      .traverse(entries) { case (key, toc, status) =>
        ckj
          .addKeyStateUpdates(Map(key -> (status, toc)))
          .valueOr(err => throw new TestFailedException(_ => Some(err.toString), None, position))
      }
      .void
  }

  def mkActivenessCheck[Key: Pretty](
      fresh: Set[Key] = Set.empty[Key],
      free: Set[Key] = Set.empty[Key],
      active: Set[Key] = Set.empty[Key],
      lock: Set[Key] = Set.empty[Key],
      prior: Set[Key] = Set.empty[Key],
  ): ActivenessCheck[Key] =
    ActivenessCheck.tryCreate(
      checkFresh = fresh,
      checkFree = free,
      checkActive = active,
      lock = lock,
      needPriorState = prior,
    )

  def mkActivenessSet(
      deact: Set[LfContractId] = Set.empty,
      useOnly: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      tfIn: Set[LfContractId] = Set.empty,
      prior: Set[LfContractId] = Set.empty,
      transferIds: Set[TransferId] = Set.empty,
      freeKeys: Set[LfGlobalKey] = Set.empty,
      assignKeys: Set[LfGlobalKey] = Set.empty,
      unassignKeys: Set[LfGlobalKey] = Set.empty,
  ): ActivenessSet = {
    val contracts = ActivenessCheck.tryCreate(
      checkFresh = create,
      checkFree = tfIn,
      checkActive = deact ++ useOnly,
      lock = create ++ tfIn ++ deact,
      needPriorState = prior,
    )
    val keys = ActivenessCheck.tryCreate(
      checkFresh = Set.empty,
      checkFree = freeKeys ++ assignKeys,
      checkActive =
        Set.empty, // We don't check that assigned contract keys are active during conflict detection
      lock = assignKeys ++ unassignKeys,
      needPriorState = Set.empty,
    )
    ActivenessSet(
      contracts = contracts,
      transferIds = transferIds,
      keys = keys,
    )
  }

  def mkActivenessCheckResult[Key: Pretty, Status <: PrettyPrinting](
      locked: Set[Key] = Set.empty[Key],
      notFresh: Set[Key] = Set.empty[Key],
      unknown: Set[Key] = Set.empty[Key],
      notFree: Map[Key, Status] = Map.empty[Key, Status],
      notActive: Map[Key, Status] = Map.empty[Key, Status],
      prior: Map[Key, Option[Status]] = Map.empty[Key, Option[Status]],
  ): ActivenessCheckResult[Key, Status] =
    ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
      priorStates = prior,
    )

  def mkActivenessResult(
      locked: Set[LfContractId] = Set.empty,
      notFresh: Set[LfContractId] = Set.empty,
      unknown: Set[LfContractId] = Set.empty,
      notFree: Map[LfContractId, ActiveContractStore.Status] = Map.empty,
      notActive: Map[LfContractId, ActiveContractStore.Status] = Map.empty,
      prior: Map[LfContractId, Option[ActiveContractStore.Status]] = Map.empty,
      inactiveTransfers: Set[TransferId] = Set.empty,
      lockedKeys: Set[LfGlobalKey] = Set.empty,
      unknownKeys: Set[LfGlobalKey] = Set.empty,
      notFreeKeys: Map[LfGlobalKey, ContractKeyJournal.Status] = Map.empty,
      notActiveKeys: Map[LfGlobalKey, ContractKeyJournal.Status] = Map.empty,
  ): ActivenessResult = {
    val contracts = ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
      priorStates = prior,
    )
    val keys = ActivenessCheckResult(
      alreadyLocked = lockedKeys,
      notFresh = Set.empty,
      unknown = unknownKeys,
      notFree = notFreeKeys,
      notActive = notActiveKeys,
      priorStates = Map.empty[LfGlobalKey, Option[ContractKeyJournal.Status]],
    )
    ActivenessResult(
      contracts = contracts,
      inactiveTransfers = inactiveTransfers,
      keys = keys,
    )
  }

  def mkCommitSet(
      arch: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      tfOut: Map[LfContractId, (DomainId, TransferCounterO)] = Map.empty,
      tfIn: Map[LfContractId, TransferId] = Map.empty,
      keys: Map[LfGlobalKey, ContractKeyJournal.Status] = Map.empty,
  ): CommitSet = {
    val contractHash = ExampleTransactionFactory.lfHash(0)
    CommitSet(
      archivals = arch
        .map(
          _ -> WithContractHash(
            CommitSet.ArchivalCommit(Set.empty[LfPartyId]),
            contractHash,
          )
        )
        .toMap,
      creations = create
        .map(
          _ -> WithContractHash(
            CommitSet.CreationCommit(
              ContractMetadata.empty,
              initialTransferCounter,
            ),
            contractHash,
          )
        )
        .toMap,
      transferOuts = tfOut.fmap { case (id, transferCounter) =>
        WithContractHash(
          CommitSet.TransferOutCommit(
            TargetDomainId(id),
            Set.empty,
            transferCounter,
          ),
          contractHash,
        )
      },
      transferIns = tfIn.fmap(id =>
        WithContractHash(
          CommitSet.TransferInCommit(
            id,
            ContractMetadata.empty,
            initialTransferCounter,
          ),
          contractHash,
        )
      ),
      keyUpdates = keys,
    )
  }
}
