// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Foldable
import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.AcsInspection.*
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.messages.HasDomainId
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherUtil, MonadUtil}
import com.digitalasset.canton.{LfPartyId, TransferCounter}

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class AcsInspection(
    requestJournalStore: RequestJournalStore,
    val activeContractStore: ActiveContractStore,
    val contractStore: ContractStore,
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
    * @return A snapshot (with its timestamp) or None if no clean timestamp is known
    */
  def getCurrentSnapshot()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]]]] =
    for {
      cursorHeadO <- requestJournalStore.preheadClean
      snapshot <- cursorHeadO
        .traverse { cursorHead =>
          val ts = cursorHead.timestamp
          val snapshotF = activeContractStore
            .snapshot(ts)
            .map(_.map { case (id, (timestamp, transferCounter)) =>
              id -> (timestamp, transferCounter)
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
  ): EitherT[Future, AcsInspectionError, AcsSnapshot[
    SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]
  ]] =
    for {
      _ <-
        if (!skipCleanTimestampCheck)
          TimestampValidation.beforePrehead(
            domainId,
            requestJournalStore.preheadClean,
            timestamp,
          )
        else EitherT.pure[Future, AcsInspectionError](())
      snapshot <- EitherT.right(activeContractStore.snapshot(timestamp))
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
  ): EitherT[Future, AcsInspectionError, Option[
    AcsSnapshot[Iterator[Seq[(LfContractId, TransferCounter)]]]
  ]] = {

    type MaybeSnapshot =
      Option[AcsSnapshot[SortedMap[LfContractId, (CantonTimestamp, TransferCounter)]]]

    val maybeSnapshotET: EitherT[Future, AcsInspectionError, MaybeSnapshot] = timestamp match {
      case Some(timestamp) =>
        getSnapshotAt(domainId)(timestamp, skipCleanTimestampCheck = skipCleanTimestampCheck)
          .map(Some(_))

      case None =>
        EitherT.right[AcsInspectionError](getCurrentSnapshot())
    }

    maybeSnapshotET.map(
      _.map { case AcsSnapshot(snapshot, ts) =>
        val groupedSnapshot = snapshot.iterator
          .map { case (cid, (_, transferCounter)) =>
            cid -> transferCounter
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
  ): EitherT[Future, AcsInspectionError, Unit] =
    for {
      topologySnapshot <- EitherT.right[AcsInspectionError](
        topologyClient.awaitSnapshot(snapshotTs)
      )
      hostedStakeholders <-
        EitherT.right[AcsInspectionError](
          topologySnapshot
            .hostedOn(allStakeholders, participantId)
            .map(_.keysIterator.toSeq)
        )

      remainingHostedStakeholders = hostedStakeholders.diff(offboardedParties.toSeq)

      _ <- EitherT.cond[Future](
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
  )(f: (SerializableContract, TransferCounter) => Either[AcsInspectionError, Unit])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, AcsInspectionError, Option[(Set[LfPartyId], CantonTimestamp)]] =
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
    * @return The union of all stakeholders of all contracts on which `f` was applied
    */
  private def forEachBatch(
      domainId: DomainId,
      parties: Set[LfPartyId],
      f: (SerializableContract, TransferCounter) => Either[AcsInspectionError, Unit],
  )(batch: Seq[(LfContractId, TransferCounter)])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, AcsInspectionError, Set[LfPartyId]] = {
    val (cids, transferCounters) = batch.unzip

    val allStakeholders: mutable.Set[LfPartyId] = mutable.Set()

    for {
      batch <- contractStore
        .lookupManyExistingUncached(cids)
        .leftMap(missingContract =>
          AcsInspectionError.InconsistentSnapshot(domainId, missingContract)
        )

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

object AcsInspection {

  private val BatchSize = PositiveInt.tryCreate(1000)

  final case class AcsSnapshot[S](snapshot: S, ts: CantonTimestamp) // timestamp of the snapshot

  object TimestampValidation {

    private def validate[A, F[_]: Foldable](
        ffa: Future[F[A]]
    )(p: A => Boolean)(fail: A => AcsInspectionError)(implicit
        ec: ExecutionContext
    ): EitherT[Future, AcsInspectionError, Unit] =
      EitherT(ffa.map(_.traverse_(a => EitherUtil.condUnitE(p(a), fail(a)))))

    def beforePrehead(
        domainId: DomainId,
        cursorPrehead: Future[Option[RequestCounterCursorPrehead]],
        timestamp: CantonTimestamp,
    )(implicit ec: ExecutionContext): EitherT[Future, AcsInspectionError, Unit] =
      validate(cursorPrehead)(timestamp < _.timestamp)(cp =>
        AcsInspectionError.TimestampAfterPrehead(domainId, timestamp, cp.timestamp)
      )

    def afterPruning(
        domainId: DomainId,
        pruningStatus: Future[Option[PruningStatus]],
        timestamp: CantonTimestamp,
    )(implicit
        ec: ExecutionContext
    ): EitherT[Future, AcsInspectionError, Unit] =
      validate(pruningStatus)(timestamp >= _.timestamp)(ps =>
        AcsInspectionError.TimestampBeforePruning(domainId, timestamp, ps.timestamp)
      )

  }
}

sealed abstract class AcsInspectionError extends Product with Serializable with HasDomainId {}

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
