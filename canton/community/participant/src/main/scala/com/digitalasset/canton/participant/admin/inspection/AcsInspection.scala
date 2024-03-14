// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{LfPartyId, TransferCounter}

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[inspection] object AcsInspection {

  private val BatchSize = PositiveInt.tryCreate(1000)

  final case class AcsSnapshot[S](
      snapshot: S,
      ts: CantonTimestamp, // timestamp of the snapshot
  )

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
    getCurrentSnapshot(state)
      .flatMap(_.traverse { acs =>
        state.contractStore
          .find(filterId, filterPackage, filterTemplate, limit)
          .map(_.map(sc => (acs.snapshot.contains(sc.contractId), sc)))
      })
      .map(_.getOrElse(Nil))

  def hasActiveContracts(state: SyncDomainPersistentState, partyId: PartyId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Boolean] =
    for {
      acsSnapshotO <- getCurrentSnapshot(state)
      res <- acsSnapshotO.fold(Future.successful(false))(acsSnapshot =>
        state.contractStore.hasActiveContracts(partyId, acsSnapshot.snapshot.keysIterator)
      )
    } yield res

  /** Get the current snapshot
    * @return A snapshot (with its timestamp) or None if no clean timestamp is known
    */
  def getCurrentSnapshot(state: SyncDomainPersistentState)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]]]] =
    for {
      cursorHeadO <- state.requestJournalStore.preheadClean
      snapshot <- cursorHeadO
        .traverse { cursorHead =>
          val ts = cursorHead.timestamp
          val snapshotF = state.activeContractStore
            .snapshot(ts)
            .map(_.map { case (id, (timestamp, transferCounter)) =>
              id -> (timestamp, transferCounter)
            })

          snapshotF.map(snapshot => Some(AcsSnapshot(snapshot, ts)))
        }
        .map(_.flatten)
    } yield snapshot

  // fetch acs, checking that the requested timestamp is clean
  private def getSnapshotAt(domainId: DomainId, state: SyncDomainPersistentState)(
      timestamp: CantonTimestamp,
      skipCleanTimestampCheck: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, AcsSnapshot[
    SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]
  ]] =
    for {
      _ <-
        if (!skipCleanTimestampCheck)
          TimestampValidation.beforePrehead(
            domainId,
            state.requestJournalStore.preheadClean,
            timestamp,
          )
        else EitherT.pure[Future, Error](())
      snapshot <- EitherT.right(state.activeContractStore.snapshot(timestamp))
      // check after getting the snapshot in case a pruning was happening concurrently
      _ <- TimestampValidation.afterPruning(
        domainId,
        state.activeContractStore.pruningStatus,
        timestamp,
      )
    } yield AcsSnapshot(snapshot, timestamp)

  // sort acs for easier comparison
  private def getAcsSnapshot(
      domainId: DomainId,
      state: SyncDomainPersistentState,
      timestamp: Option[CantonTimestamp],
      skipCleanTimestampCheck: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, Option[
    AcsSnapshot[Iterator[Seq[(LfContractId, TransferCounter)]]]
  ]] = {

    type MaybeSnapshot =
      Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]]]

    val maybeSnapshotET: EitherT[Future, Error, MaybeSnapshot] = timestamp match {
      case Some(timestamp) =>
        getSnapshotAt(domainId, state)(timestamp, skipCleanTimestampCheck = skipCleanTimestampCheck)
          .map(Some(_))

      case None =>
        EitherT.liftF[Future, Error, MaybeSnapshot](getCurrentSnapshot(state))
    }

    maybeSnapshotET.map(
      _.map { case AcsSnapshot(snapshot, ts) =>
        val groupedSnapshot = snapshot.iterator
          .map { case (cid, (_, transferCounter)) =>
            cid -> transferCounter
          }
          .toSeq
          .grouped(
            AcsInspection.BatchSize.value
          ) // TODO(#14818): Batching should be done by the caller not here))

        AcsSnapshot(groupedSnapshot, ts)
      }
    )
  }

  def forEachVisibleActiveContract(
      domainId: DomainId,
      state: SyncDomainPersistentState,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      skipCleanTimestampCheck: Boolean = false,
  )(f: (SerializableContract, TransferCounter) => Either[Error, Unit])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, Option[(Set[LfPartyId], CantonTimestamp)]] = {
    for {
      acsSnapshotO <- getAcsSnapshot(
        domainId,
        state,
        timestamp,
        skipCleanTimestampCheck = skipCleanTimestampCheck,
      )
      allStakeholdersAndTs <- acsSnapshotO.traverse { acsSnapshot =>
        MonadUtil
          .sequentialTraverseMonoid(acsSnapshot.snapshot)(
            forEachBatch(domainId, state, parties, f)
          )
          .map((_, acsSnapshot.ts))
      }
    } yield allStakeholdersAndTs
  }

  /** Check that the ACS snapshot does not contain contracts that are still needed on the participant.
    * In the context of party offboarding, we want to avoid purging contracts
    * that are needed for other parties hosted on the participant.
    */
  def checkOffboardingSnapshot(
      participantId: ParticipantId,
      offboardedParties: Set[LfPartyId],
      allStakeholders: Set[LfPartyId],
      snapshotTs: CantonTimestamp,
      topologyClient: DomainTopologyClient,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] = {
    for {
      topologySnapshot <- EitherT.liftF[Future, String, TopologySnapshot](
        topologyClient.awaitSnapshot(snapshotTs)
      )

      hostedStakeholders <-
        EitherT.liftF[Future, String, Seq[LfPartyId]](
          topologySnapshot
            .hostedOn(allStakeholders, participantId)
            .map(_.keysIterator.toSeq)
        )

      remainingHostedStakeholders = hostedStakeholders.diff(offboardedParties.toSeq)

      _ <- EitherT.cond[Future](
        remainingHostedStakeholders.isEmpty,
        (),
        s"Cannot take snapshot to offboard parties ${offboardedParties.toSeq} at $snapshotTs, because the following parties have contracts: ${remainingHostedStakeholders
            .mkString(", ")}",
      )
    } yield ()
  }

  /** Applies function f to all the contracts in the batch whose set of stakeholders has
    * non-empty intersection with `parties`
    * @return The union of all stakeholders of all contracts on which `f` was applied
    */
  private def forEachBatch(
      domainId: DomainId,
      state: SyncDomainPersistentState,
      parties: Set[LfPartyId],
      f: (SerializableContract, TransferCounter) => Either[Error, Unit],
  )(batch: Seq[(LfContractId, TransferCounter)])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, Error, Set[LfPartyId]] = {
    val (cids, transferCounters) = batch.unzip

    val allStakeholders: mutable.Set[LfPartyId] = mutable.Set()

    for {
      batch <- state.contractStore
        .lookupManyUncached(cids)
        .leftMap(missingContract => Error.InconsistentSnapshot(domainId, missingContract))

      contractsWithTransferCounter = batch.zip(transferCounters)

      stakeholdersE = contractsWithTransferCounter
        .traverse_ { case (storedContract, transferCounter) =>
          if (parties.exists(storedContract.contract.metadata.stakeholders)) {
            allStakeholders ++= storedContract.contract.metadata.stakeholders
            f(storedContract.contract, transferCounter)
          } else
            Right(())
        }
        .map(_ => allStakeholders.toSet)

      allStakeholders <- EitherT.fromEither[Future](stakeholdersE)
    } yield allStakeholders
  }
}
