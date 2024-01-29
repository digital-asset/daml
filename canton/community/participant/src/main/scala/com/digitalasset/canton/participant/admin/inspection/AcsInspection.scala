// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.data.EitherT
import cats.syntax.foldable.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.{StoredContract, SyncDomainPersistentState}
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

private[inspection] object AcsInspection {

  private val BatchSize = PositiveInt.tryCreate(1000)

  def findContracts(
      state: SyncDomainPersistentState,
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[List[(Boolean, SerializableContract)]] =
    for {
      acs <- getCurrentSnapshot(state)
      contracts <- state.contractStore
        .find(filterId, filterPackage, filterTemplate, limit)
        .map(_.map(sc => (acs.contains(sc.contractId), sc)))
    } yield contracts

  def hasActiveContracts(state: SyncDomainPersistentState, partyId: PartyId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Boolean] =
    for {
      acs <- getCurrentSnapshot(state)
      res <- state.contractStore.hasActiveContracts(partyId, acs.keysIterator)
    } yield res

  def getCurrentSnapshot(state: SyncDomainPersistentState)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[SortedMap[LfContractId, CantonTimestamp]] =
    for {
      cursorHeadO <- state.requestJournalStore.preheadClean
      snapshot <- cursorHeadO.fold(
        Future.successful(SortedMap.empty[LfContractId, CantonTimestamp])
      )(cursorHead =>
        state.activeContractStore
          .snapshot(cursorHead.timestamp)
      )
    } yield snapshot

  // fetch acs, checking that the requested timestamp is clean
  private def getSnapshotAt(domainId: DomainId, state: SyncDomainPersistentState)(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, SortedMap[LfContractId, CantonTimestamp]] =
    for {
      _ <- TimestampValidation.beforePrehead(
        domainId,
        state.requestJournalStore.preheadClean,
        timestamp,
      )
      snapshot <- EitherT.right(state.activeContractStore.snapshot(timestamp))
      // check after getting the snapshot in case a pruning was happening concurrently
      _ <- TimestampValidation.afterPruning(
        domainId,
        state.activeContractStore.pruningStatus,
        timestamp,
      )
    } yield snapshot

  // sort acs for easier comparison
  private def getAcsSnapshot(
      domainId: DomainId,
      state: SyncDomainPersistentState,
      timestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, Iterator[Seq[LfContractId]]] =
    timestamp
      .map(getSnapshotAt(domainId, state))
      .getOrElse(EitherT.right(getCurrentSnapshot(state)))
      .map(
        _.iterator
          .map { case (cid, _) => cid }
          .toSeq
          .grouped(
            AcsInspection.BatchSize.value
          ) // TODO(#14818): Batching should be done by the caller not here
      )

  def forEachVisibleActiveContract(
      domainId: DomainId,
      state: SyncDomainPersistentState,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
  )(f: SerializableContract => Either[Error, Unit])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, Unit] =
    for {
      acs <- getAcsSnapshot(domainId, state, timestamp)
      unit <- MonadUtil.sequentialTraverse_(acs)(forEachBatch(domainId, state, parties, f))
    } yield unit

  private def forEachBatch(
      domainId: DomainId,
      state: SyncDomainPersistentState,
      parties: Set[LfPartyId],
      f: SerializableContract => Either[Error, Unit],
  )(cids: Seq[LfContractId])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, Unit] = {
    for {
      batch <- state.contractStore
        .lookupManyUncached(cids)
        .leftMap(missingContract => Error.InconsistentSnapshot(domainId, missingContract))

      _ <- EitherT.fromEither[Future](applyToBatch(parties, f)(batch))
    } yield ()
  }

  private def applyToBatch(
      parties: Set[LfPartyId],
      f: SerializableContract => Either[Error, Unit],
  )(batch: List[StoredContract]): Either[Error, Unit] =
    batch.traverse_ { case storedContract =>
      if (parties.exists(storedContract.contract.metadata.stakeholders))
        f(storedContract.contract)
      else
        Right(())
    }

}
