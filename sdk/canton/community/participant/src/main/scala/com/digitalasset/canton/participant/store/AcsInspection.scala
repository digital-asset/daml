// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import cats.{Eval, Foldable}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.RequestIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.AcsInspection.*
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.messages.HasSynchronizerId
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class AcsInspection(
    synchronizerId: SynchronizerId,
    val activeContractStore: ActiveContractStore,
    val contractStore: ContractStore,
    val ledgerApiStore: Eval[LedgerApiStore],
) {
  def findContracts(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[List[(Boolean, SerializableContract)]] =
    getCurrentSnapshot()
      .flatMap(_.traverse { acs =>
        contractStore
          .find(filterId, filterPackage, filterTemplate, limit)
          .map(_.map(sc => (acs.snapshot.contains(sc.contractId), sc)))
      })
      .map(_.getOrElse(Nil))

  def findContractPayloads(
      contractIds: NonEmpty[Seq[LfContractId]],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Map[LfContractId, SerializableContract]
  ] = contractStore.findWithPayload(contractIds, limit)

  def hasActiveContracts(partyId: PartyId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Boolean] =
    for {
      acsSnapshotO <- getCurrentSnapshot()
      res <- acsSnapshotO.fold(FutureUnlessShutdown.pure(false))(acsSnapshot =>
        contractStore.hasActiveContracts(partyId, acsSnapshot.snapshot.keysIterator)
      )
    } yield res

  /** Get the current snapshot
    *
    * @return A snapshot (with its timestamp) or None if no clean timestamp is known
    */
  def getCurrentSnapshot()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[
    Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]]]
  ] =
    for {
      requestIndex <- ledgerApiStore.value
        .cleanSynchronizerIndex(synchronizerId)
        .map(_.flatMap(_.requestIndex))
      snapshot <-
        requestIndex
          .traverse { cursorHead =>
            val ts = cursorHead.timestamp
            val snapshotF = activeContractStore
              .snapshot(ts)
              .map(_.map { case (id, (timestamp, reassignmentCounter)) =>
                id -> (timestamp, reassignmentCounter)
              })

            snapshotF.map(snapshot => Some(AcsSnapshot(snapshot, ts)))
          }
          .map(_.flatten)
    } yield snapshot

  // fetch acs, checking that the requested timestamp is clean
  private def getSnapshotAt(synchronizerId: SynchronizerId)(
      timestamp: CantonTimestamp,
      skipCleanTimestampCheck: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, AcsSnapshot[
    SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]
  ]] =
    for {
      _ <-
        if (!skipCleanTimestampCheck)
          TimestampValidation
            .beforeRequestIndex(
              synchronizerId,
              ledgerApiStore.value
                .cleanSynchronizerIndex(synchronizerId)
                .map(_.flatMap(_.requestIndex)),
              timestamp,
            )
        else EitherT.pure[FutureUnlessShutdown, AcsInspectionError](())
      snapshot <- EitherT
        .right(activeContractStore.snapshot(timestamp))
      // check after getting the snapshot in case a pruning was happening concurrently
      _ <- TimestampValidation.afterPruning(
        synchronizerId,
        activeContractStore.pruningStatus,
        timestamp,
      )
    } yield AcsSnapshot(snapshot, timestamp)

  // sort acs for easier comparison
  private def getAcsSnapshot(
      synchronizerId: SynchronizerId,
      timestamp: Option[CantonTimestamp],
      skipCleanTimestampCheck: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Option[
    AcsSnapshot[Iterator[Seq[(LfContractId, ReassignmentCounter)]]]
  ]] = {

    type MaybeSnapshot =
      Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]]]

    val maybeSnapshotET: EitherT[FutureUnlessShutdown, AcsInspectionError, MaybeSnapshot] =
      timestamp match {
        case Some(timestamp) =>
          getSnapshotAt(synchronizerId)(
            timestamp,
            skipCleanTimestampCheck = skipCleanTimestampCheck,
          )
            .map(Some(_))

        case None =>
          EitherT
            .right[AcsInspectionError](getCurrentSnapshot())
      }

    maybeSnapshotET.map(
      _.map { case AcsSnapshot(snapshot, ts) =>
        val groupedSnapshot = snapshot.iterator
          .map { case (cid, (_, reassignmentCounter)) =>
            cid -> reassignmentCounter
          }
          .toSeq
          .grouped(
            BatchSize.value
          ) // TODO(#14818): Batching should be done by the caller not here))

        AcsSnapshot(groupedSnapshot, ts)
      }
    )
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
      topologyClient: SynchronizerTopologyClient,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
    for {
      topologySnapshot <- EitherT.right[AcsInspectionError](
        topologyClient.awaitSnapshot(snapshotTs)
      )
      hostedStakeholders <-
        EitherT
          .right[AcsInspectionError](
            topologySnapshot
              .hostedOn(allStakeholders, participantId)
              .map(_.keysIterator.toSeq)
          )

      remainingHostedStakeholders = hostedStakeholders.diff(offboardedParties.toSeq)

      _ <- EitherT.cond[FutureUnlessShutdown](
        remainingHostedStakeholders.isEmpty,
        (),
        AcsInspectionError.OffboardingParty(
          topologyClient.synchronizerId,
          s"Cannot take snapshot to offboard parties ${offboardedParties.toSeq} at $snapshotTs, because the following parties have contracts: ${remainingHostedStakeholders
              .mkString(", ")}",
        ): AcsInspectionError,
      )
    } yield ()

  def forEachVisibleActiveContract(
      synchronizerId: SynchronizerId,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      skipCleanTimestampCheck: Boolean = false,
  )(f: (SerializableContract, ReassignmentCounter) => Either[AcsInspectionError, Unit])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Option[(Set[LfPartyId], CantonTimestamp)]] =
    for {
      acsSnapshotO <- getAcsSnapshot(
        synchronizerId,
        timestamp,
        skipCleanTimestampCheck = skipCleanTimestampCheck,
      )
      allStakeholdersAndTs <- acsSnapshotO.traverse { acsSnapshot =>
        MonadUtil
          .sequentialTraverseMonoid(acsSnapshot.snapshot)(
            forEachBatch(synchronizerId, parties, f)
          )
          .map((_, acsSnapshot.ts))
      }
    } yield allStakeholdersAndTs

  /** Applies function f to all the contracts in the batch whose set of stakeholders has
    * non-empty intersection with `parties`
    *
    * @return The union of all stakeholders of all contracts on which `f` was applied
    */
  private def forEachBatch(
      synchronizerId: SynchronizerId,
      parties: Set[LfPartyId],
      f: (SerializableContract, ReassignmentCounter) => Either[AcsInspectionError, Unit],
  )(batch: Seq[(LfContractId, ReassignmentCounter)])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Set[LfPartyId]] = {
    val (cids, reassignmentCounters) = batch.unzip

    val allStakeholders: mutable.Set[LfPartyId] = mutable.Set()

    for {
      batch <- contractStore
        .lookupManyExistingUncached(cids)
        .leftMap(missingContract =>
          AcsInspectionError.InconsistentSnapshot(synchronizerId, missingContract)
        )

      contractsWithReassignmentCounter = batch.zip(reassignmentCounters)

      stakeholdersE = contractsWithReassignmentCounter
        .traverse_ { case (contract, reassignmentCounter) =>
          if (parties.exists(contract.metadata.stakeholders)) {
            allStakeholders ++= contract.metadata.stakeholders
            f(contract, reassignmentCounter)
          } else
            Either.unit
        }
        .map(_ => allStakeholders.toSet)

      allStakeholders <- EitherT.fromEither[FutureUnlessShutdown](stakeholdersE)
    } yield allStakeholders
  }
}

object AcsInspection {

  private val BatchSize = PositiveInt.tryCreate(1000)

  final case class AcsSnapshot[S](snapshot: S, ts: CantonTimestamp) // timestamp of the snapshot

  object TimestampValidation {

    private def validate[A, F[_]: Foldable](
        ffa: FutureUnlessShutdown[F[A]]
    )(p: A => Boolean)(fail: A => AcsInspectionError)(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
      EitherT(ffa.map(_.traverse_(a => Either.cond(p(a), (), fail(a)))))

    def beforeRequestIndex(
        synchronizerId: SynchronizerId,
        requestIndex: FutureUnlessShutdown[Option[RequestIndex]],
        timestamp: CantonTimestamp,
    )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
      validate(requestIndex)(timestamp < _.timestamp)(cp =>
        AcsInspectionError.TimestampAfterCleanRequestIndex(synchronizerId, timestamp, cp.timestamp)
      )

    def afterPruning(
        synchronizerId: SynchronizerId,
        pruningStatus: FutureUnlessShutdown[Option[PruningStatus]],
        timestamp: CantonTimestamp,
    )(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
      validate(pruningStatus)(
        timestamp >= _.timestamp
      )(ps => AcsInspectionError.TimestampBeforePruning(synchronizerId, timestamp, ps.timestamp))

  }
}

sealed abstract class AcsInspectionError extends Product with Serializable with HasSynchronizerId

object AcsInspectionError {
  final case class TimestampAfterCleanRequestIndex(
      override val synchronizerId: SynchronizerId,
      requestedTimestamp: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
  ) extends AcsInspectionError

  final case class TimestampBeforePruning(
      override val synchronizerId: SynchronizerId,
      requestedTimestamp: CantonTimestamp,
      prunedTimestamp: CantonTimestamp,
  ) extends AcsInspectionError

  final case class InconsistentSnapshot(
      override val synchronizerId: SynchronizerId,
      missingContract: LfContractId,
  ) extends AcsInspectionError

  final case class InvariantIssue(
      override val synchronizerId: SynchronizerId,
      contract: LfContractId,
      errorMessage: String,
  ) extends AcsInspectionError

  final case class OffboardingParty(synchronizerId: SynchronizerId, error: String)
      extends AcsInspectionError
  final case class SerializationIssue(
      override val synchronizerId: SynchronizerId,
      contract: LfContractId,
      errorMessage: String,
  ) extends AcsInspectionError

  final case class ContractLookupIssue(
      synchronizerId: SynchronizerId,
      contracts: Seq[LfContractId],
      errorMessage: String,
  ) extends AcsInspectionError
}
