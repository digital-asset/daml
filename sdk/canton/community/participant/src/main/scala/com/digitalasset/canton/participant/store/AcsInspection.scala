// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import cats.{Eval, Foldable}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.RequestIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.AcsInspection.*
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.messages.HasDomainId
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class AcsInspection(
    domainId: DomainId,
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
  ): Future[List[(Boolean, SerializableContract)]] =
    getCurrentSnapshot()
      .flatMap(_.traverse { acs =>
        contractStore
          .find(filterId, filterPackage, filterTemplate, limit)
          .map(_.map(sc => (acs.snapshot.contains(sc.contractId), sc)))
      })
      .map(_.getOrElse(Nil))

  def hasActiveContracts(partyId: PartyId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Boolean] =
    for {
      acsSnapshotO <- getCurrentSnapshot()
      res <- acsSnapshotO.fold(Future.successful(false))(acsSnapshot =>
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
  ): Future[Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]]]] =
    for {
      requestIndex <- ledgerApiStore.value.domainIndex(domainId).map(_.requestIndex)
      snapshot <- requestIndex
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
  private def getSnapshotAt(domainId: DomainId)(
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
              domainId,
              ledgerApiStore.value.domainIndex(domainId).map(_.requestIndex),
              timestamp,
            )
            .mapK(FutureUnlessShutdown.outcomeK)
        else EitherT.pure[FutureUnlessShutdown, AcsInspectionError](())
      snapshot <- EitherT
        .right(activeContractStore.snapshot(timestamp))
        .mapK(FutureUnlessShutdown.outcomeK)
      // check after getting the snapshot in case a pruning was happening concurrently
      _ <- TimestampValidation.afterPruning(
        domainId,
        activeContractStore.pruningStatus,
        timestamp,
      )
    } yield AcsSnapshot(snapshot, timestamp)

  // sort acs for easier comparison
  private def getAcsSnapshot(
      domainId: DomainId,
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
          getSnapshotAt(domainId)(timestamp, skipCleanTimestampCheck = skipCleanTimestampCheck)
            .map(Some(_))

        case None =>
          EitherT
            .right[AcsInspectionError](getCurrentSnapshot())
            .mapK(FutureUnlessShutdown.outcomeK)
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
      topologyClient: DomainTopologyClient,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
    for {
      topologySnapshot <- EitherT.right[AcsInspectionError](
        topologyClient.awaitSnapshotUS(snapshotTs)
      )
      hostedStakeholders <-
        EitherT
          .right[AcsInspectionError](
            topologySnapshot
              .hostedOn(allStakeholders, participantId)
              .map(_.keysIterator.toSeq)
          )
          .mapK(FutureUnlessShutdown.outcomeK)

      remainingHostedStakeholders = hostedStakeholders.diff(offboardedParties.toSeq)

      _ <- EitherT.cond[FutureUnlessShutdown](
        remainingHostedStakeholders.isEmpty,
        (),
        AcsInspectionError.OffboardingParty(
          topologyClient.domainId,
          s"Cannot take snapshot to offboard parties ${offboardedParties.toSeq} at $snapshotTs, because the following parties have contracts: ${remainingHostedStakeholders
              .mkString(", ")}",
        ): AcsInspectionError,
      )
    } yield ()

  def forEachVisibleActiveContract(
      domainId: DomainId,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      skipCleanTimestampCheck: Boolean = false,
  )(f: (SerializableContract, ReassignmentCounter) => Either[AcsInspectionError, Unit])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Option[(Set[LfPartyId], CantonTimestamp)]] =
    for {
      acsSnapshotO <- getAcsSnapshot(
        domainId,
        timestamp,
        skipCleanTimestampCheck = skipCleanTimestampCheck,
      )
      allStakeholdersAndTs <- acsSnapshotO.traverse { acsSnapshot =>
        MonadUtil
          .sequentialTraverseMonoid(acsSnapshot.snapshot)(
            forEachBatch(domainId, parties, f)
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
      domainId: DomainId,
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
          AcsInspectionError.InconsistentSnapshot(domainId, missingContract)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      contractsWithReassignmentCounter = batch.zip(reassignmentCounters)

      stakeholdersE = contractsWithReassignmentCounter
        .traverse_ { case (storedContract, reassignmentCounter) =>
          if (parties.exists(storedContract.contract.metadata.stakeholders)) {
            allStakeholders ++= storedContract.contract.metadata.stakeholders
            f(storedContract.contract, reassignmentCounter)
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
        ffa: Future[F[A]]
    )(p: A => Boolean)(fail: A => AcsInspectionError)(implicit
        ec: ExecutionContext
    ): EitherT[Future, AcsInspectionError, Unit] =
      EitherT(ffa.map(_.traverse_(a => Either.cond(p(a), (), fail(a)))))

    private def validateUS[A, F[_]: Foldable](
        ffa: FutureUnlessShutdown[F[A]]
    )(p: A => Boolean)(fail: A => AcsInspectionError)(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
      EitherT(ffa.map(_.traverse_(a => Either.cond(p(a), (), fail(a)))))

    def beforeRequestIndex(
        domainId: DomainId,
        requestIndex: Future[Option[RequestIndex]],
        timestamp: CantonTimestamp,
    )(implicit ec: ExecutionContext): EitherT[Future, AcsInspectionError, Unit] =
      validate(requestIndex)(timestamp < _.timestamp)(cp =>
        AcsInspectionError.TimestampAfterPrehead(domainId, timestamp, cp.timestamp)
      )

    def afterPruning(
        domainId: DomainId,
        pruningStatus: FutureUnlessShutdown[Option[PruningStatus]],
        timestamp: CantonTimestamp,
    )(implicit
        ec: ExecutionContext
    ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
      validateUS(pruningStatus)(
        timestamp >= _.timestamp
      )(ps => AcsInspectionError.TimestampBeforePruning(domainId, timestamp, ps.timestamp))

  }
}

sealed abstract class AcsInspectionError extends Product with Serializable with HasDomainId

object AcsInspectionError {
  final case class TimestampAfterPrehead(
      override val domainId: DomainId,
      requestedTimestamp: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
  ) extends AcsInspectionError

  final case class TimestampBeforePruning(
      override val domainId: DomainId,
      requestedTimestamp: CantonTimestamp,
      prunedTimestamp: CantonTimestamp,
  ) extends AcsInspectionError

  final case class InconsistentSnapshot(
      override val domainId: DomainId,
      missingContract: LfContractId,
  ) extends AcsInspectionError

  final case class InvariantIssue(
      override val domainId: DomainId,
      contract: LfContractId,
      errorMessage: String,
  ) extends AcsInspectionError

  final case class OffboardingParty(domainId: DomainId, error: String) extends AcsInspectionError
  final case class SerializationIssue(
      override val domainId: DomainId,
      contract: LfContractId,
      errorMessage: String,
  ) extends AcsInspectionError
}
