// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  Purged,
  TransferredAway,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryActiveContractStore,
  InMemoryTransferStore,
  TransferCache,
}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  TransferStore,
  TransferStoreTest,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LfPartyId,
  ScalaFuturesWithPatience,
  TransferCounter,
}
import org.scalatest.AsyncTestSuite

import scala.concurrent.{ExecutionContext, Future}

private[protocol] trait ConflictDetectionHelpers {
  this: AsyncTestSuite & BaseTest & HasExecutorService =>

  import ConflictDetectionHelpers.*

  def parallelExecutionContext: ExecutionContext = executorService

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 2)

  def mkEmptyAcs(): ActiveContractStore =
    new InMemoryActiveContractStore(indexedStringStore, testedProtocolVersion, loggerFactory)(
      parallelExecutionContext
    )

  def mkAcs(
      entries: (LfContractId, TimeOfChange, ActiveContractStore.Status)*
  )(implicit traceContext: TraceContext): Future[ActiveContractStore] = {
    val acs = mkEmptyAcs()
    insertEntriesAcs(acs, entries).map(_ => acs)
  }

  def mkTransferCache(
      loggerFactory: NamedLoggerFactory,
      store: TransferStore =
        new InMemoryTransferStore(TransferStoreTest.targetDomain, loggerFactory),
  )(
      entries: (TransferId, MediatorGroupRecipient)*
  )(implicit traceContext: TraceContext): Future[TransferCache] = {
    Future
      .traverse(entries) { case (transferId, sourceMediator) =>
        for {
          transfer <- TransferStoreTest.mkTransferDataForDomain(
            transferId,
            sourceMediator,
            targetDomainId = TransferStoreTest.targetDomain,
          )
          result <- store
            .addTransfer(transfer)
            .value
            .failOnShutdown
        } yield result
      }
      .map(_ => new TransferCache(store, loggerFactory)(parallelExecutionContext))
  }
}

private[protocol] object ConflictDetectionHelpers extends ScalaFuturesWithPatience {

  private val initialTransferCounter: TransferCounter = TransferCounter.Genesis

  def insertEntriesAcs(
      acs: ActiveContractStore,
      entries: Seq[(LfContractId, TimeOfChange, ActiveContractStore.Status)],
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    Future
      .traverse(entries) {
        case (coid, toc, Active(_transferCounter)) =>
          acs
            .markContractCreated(coid -> initialTransferCounter, toc)
            .value
        case (coid, toc, Archived) => acs.archiveContract(coid, toc).value
        case (coid, toc, Purged) => acs.purgeContracts(Seq((coid, toc))).value
        case (coid, toc, TransferredAway(targetDomain, transferCounter)) =>
          acs.transferOutContract(coid, toc, targetDomain, transferCounter).value
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
  ): ActivenessSet = {
    val contracts = ActivenessCheck.tryCreate(
      checkFresh = create,
      checkFree = tfIn,
      checkActive = deact ++ useOnly,
      lock = create ++ tfIn ++ deact,
      needPriorState = prior,
    )
    ActivenessSet(
      contracts = contracts,
      transferIds = transferIds,
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
  ): ActivenessResult = {
    val contracts = ActivenessCheckResult(
      alreadyLocked = locked,
      notFresh = notFresh,
      unknown = unknown,
      notFree = notFree,
      notActive = notActive,
      priorStates = prior,
    )
    ActivenessResult(
      contracts = contracts,
      inactiveTransfers = inactiveTransfers,
    )
  }

  def mkCommitSet(
      arch: Set[LfContractId] = Set.empty,
      create: Set[LfContractId] = Set.empty,
      tfOut: Map[LfContractId, (DomainId, TransferCounter)] = Map.empty,
      tfIn: Map[LfContractId, TransferId] = Map.empty,
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
    )
  }
}
