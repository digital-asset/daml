// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.NonEmptyChain
import cats.kernel.Order
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.data.{ActiveContractData, ActiveContractsData}
import com.digitalasset.canton.participant.store.memory.InMemoryActiveContractStore.IndividualChange.{
  add,
  purge,
}
import com.digitalasset.canton.participant.store.{
  ActivationsDeactivationsConsistencyCheck,
  ActiveContractStore,
  ContractChange,
  ContractStore,
  StateChangeType,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{
  LfContractId,
  SourceDomainId,
  TargetDomainId,
  TransferDomainId,
}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicInteger
import scala.Ordered.orderingToOrdered
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** Implements an [[ActiveContractStore!]] in memory. */
class InMemoryActiveContractStore(
    val indexedStringStore: IndexedStringStore,
    protocolVersion: ProtocolVersion,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends ActiveContractStore
    with NamedLogging
    with InMemoryPrunableByTime {

  import ActiveContractStore.*
  import InMemoryActiveContractStore.*

  /** Invariant: Never maps to [[ContractStatus.Nonexistent]] */
  private[this] val table = TrieMap.empty[LfContractId, ContractStatus]

  override def markContractsCreatedOrAdded(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val activeContractsDataE = ActiveContractsData.create(protocolVersion, toc, contracts)
    activeContractsDataE match {
      case Left(errorMessage) =>
        CheckedT.abortT(ActiveContractsDataInvariantViolation(errorMessage))
      case Right(activeContractsData) =>
        CheckedT(Future.successful {
          logger.trace(
            show"Creating contracts at ${activeContractsData.toc}: ${activeContractsData.contractIds}"
          )

          activeContractsData.asSeq.to(LazyList).traverse_ { transferableContract =>
            updateTable(
              transferableContract.contractId,
              _.addCreation(transferableContract, activeContractsData.toc, isCreation = isCreation),
            )
          }
        })
    }
  }

  override def purgeOrArchiveContracts(
      archivals: Seq[LfContractId],
      toc: TimeOfChange,
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(show"Archiving contracts at $toc: $archivals")
      archivals.to(LazyList).traverse_ { contractId =>
        updateTable(contractId, _.addArchival(contractId, toc, isArchival = isArchival))
      }
    })

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] = {
    val snapshot = table.readOnlySnapshot()

    contractIds
      .to(LazyList)
      .parTraverseFilter(contractId =>
        snapshot
          .get(contractId)
          .traverse(status => latestState(status.changes).map(_.map(contractId -> _)))
          .map(_.flatten)
      )
      .map(_.toMap)
  }

  /** Returns the latest [[ActiveContractStore.ContractState]] if any */
  private def latestState(
      changes: ChangeJournal
  )(implicit traceContext: TraceContext): Future[Option[ContractState]] =
    changes.headOption.traverse { case (change, detail) =>
      val statusF = detail match {
        case ActivenessChangeDetail.Create => Future.successful(Active)
        case _: ActivenessChangeDetail.TransferIn => Future.successful(Active)
        case ActivenessChangeDetail.Archive => Future.successful(Archived)
        case out: ActivenessChangeDetail.TransferOut =>
          domainIdFromIdx(out.remoteDomainIdx).map(domainId =>
            TransferredAway(TargetDomainId(domainId))
          )

        case ActivenessChangeDetail.Purge => Future.successful(Purged)
        case ActivenessChangeDetail.Add =>
          Future.successful(Active)
      }

      statusF.map(ContractState(_, change.toc))
    }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, CantonTimestamp]] = Future.successful {
    val snapshot = SortedMap.newBuilder[LfContractId, CantonTimestamp]
    table.foreach { case (contractId, contractStatus) =>
      contractStatus.activeBy(timestamp).foreach { activationTimestamp =>
        snapshot += (contractId -> activationTimestamp)
      }
    }
    snapshot.result()
  }

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, RequestCounter]] = Future.successful {
    val snapshot = SortedMap.newBuilder[LfContractId, RequestCounter]
    table.foreach { case (contractId, contractStatus) =>
      contractStatus.activeBy(rc).foreach { activationRc =>
        snapshot += (contractId -> activationRc)
      }
    }
    snapshot.result()
  }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] =
    Future.successful {
      contractIds
        .to(LazyList)
        .mapFilter(contractId =>
          table.get(contractId).flatMap(_.activeBy(timestamp)).map(ts => contractId -> ts)
        )
        .toMap
    }

  private def prepareTransfers(
      transfers: Seq[(LfContractId, TransferDomainId, TimeOfChange)]
  ): CheckedT[Future, AcsError, AcsWarning, Seq[
    (LfContractId, Int, TimeOfChange)
  ]] = {
    val domains = transfers.map { case (_, domain, _) => domain.unwrap }.distinct
    type PreparedTransfer = (LfContractId, Int, TimeOfChange)

    for {
      domainIndices <- getDomainIndices(domains)

      preparedTransfersE = MonadUtil.sequentialTraverse(
        transfers
      ) { case (cid, remoteDomain, toc) =>
        domainIndices
          .get(remoteDomain.unwrap)
          .toRight[AcsError](UnableToFindIndex(remoteDomain.unwrap))
          .map(domainIdx => (cid, domainIdx.index, toc))
      }

      preparedTransfers <- CheckedT.fromChecked(Checked.fromEither(preparedTransfersE)): CheckedT[
        Future,
        AcsError,
        AcsWarning,
        Seq[PreparedTransfer],
      ]
    } yield preparedTransfers
  }

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    logger.trace(s"Transferring-in contracts: $transferIns")

    for {
      preparedTransfers <- prepareTransfers(transferIns)
      _ <- CheckedT(Future.successful(preparedTransfers.to(LazyList).traverse_ {
        case (contractId, sourceDomain, toc) =>
          updateTable(contractId, _.addTransferIn(contractId, toc, sourceDomain))
      }))
    } yield ()
  }

  override def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    logger.trace(s"Transferring-out contracts: $transferOuts")

    for {
      preparedTransfers <- prepareTransfers(transferOuts)
      _ <- CheckedT(Future.successful(preparedTransfers.to(LazyList).traverse_ {
        case (contractId, sourceDomain, toc) =>
          updateTable(contractId, _.addTransferOut(contractId, toc, sourceDomain))
      }))
    } yield ()
  }

  override def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] = {
    val counter = new AtomicInteger(0)
    table.foreach { case (coid, status) =>
      status.prune(beforeAndIncluding) match {
        case None =>
          counter.incrementAndGet()
          table.remove(coid).discard
        case Some(unchangedStatus) if unchangedStatus eq status => ()
        case Some(newStatus) =>
          val succeed = table.replace(coid, status, newStatus)
          if (!succeed)
            logger.warn(
              s"Active contract store modified at contract $coid while pruning requests. Skipping"
            )
      }
    }
    Future.successful(counter.get())
  }

  override def deleteSince(
      criterion: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      table.foreach { case (coid, status) =>
        val newStatus = status.deleteSince(criterion)
        if (!(newStatus eq status)) {
          val succeed = table.replace(coid, status, newStatus)
          if (!succeed)
            throw new ConcurrentModificationException(
              s"Active contract store modified at contract $coid while deleting requests."
            )
        }
      }
    }

  override def contractCount(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] =
    Future.successful(table.values.count {
      // As the changes are ordered in the reverse order of timestamps, we take the last key here.
      status =>
        status.changes.lastKey.toc.timestamp <= timestamp
    })

  private[this] def updateTable(
      contractId: LfContractId,
      f: ContractStatus => Checked[AcsError, AcsWarning, ContractStatus],
  ): Checked[AcsError, AcsWarning, Unit] =
    MapsUtil.updateWithConcurrentlyChecked_(table, contractId, f(ContractStatus.Nonexistent), f)

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    Future.successful {
      ErrorUtil.requireArgument(
        fromExclusive <= toInclusive,
        s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
      )

      val changesByToc
          : Map[TimeOfChange, List[(LfContractId, ActivenessChange, ActivenessChangeDetail)]] =
        table.toList
          .flatMap { case (coid, status) =>
            status.changes
              .filter { case (ch, _) => ch.toc > fromExclusive && ch.toc <= toInclusive }
              .toList
              .map { case (activenessChange, activenessChangeDetail) =>
                (coid, activenessChange, activenessChangeDetail)
              }
          }
          .groupBy(_._2.toc)

      val byTsAndChangeType
          : Map[TimeOfChange, Map[Boolean, List[(LfContractId, StateChangeType)]]] = changesByToc
        .fmap(_.groupBy(_._2.isActivation).fmap(_.map {

          case (coid, activenessChange, activenessChangeDetail) =>
            val stateChange =
              if (activenessChange.isActivation && !activenessChangeDetail.isTransfer)
                StateChangeType(ContractChange.Created)
              else if (activenessChange.isActivation && activenessChangeDetail.isTransfer)
                StateChangeType(ContractChange.TransferIn)
              else if (!activenessChange.isActivation && activenessChangeDetail.isTransfer)
                StateChangeType(ContractChange.TransferOut)
              else
                StateChangeType(ContractChange.Archived)
            (coid, stateChange)
        }))

      byTsAndChangeType
        .to(LazyList)
        .sortBy { case (timeOfChange, _) => timeOfChange }
        .map { case (toc, changes) =>
          val activatedIds = changes.getOrElse(true, Iterable.empty).toMap
          val deactivatedIds = changes.getOrElse(false, Iterable.empty).toMap
          (
            toc,
            ActiveContractIdsChange(
              activations = activatedIds,
              deactivations = deactivatedIds,
            ),
          )
        }
    }

  override def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[(LfContractId)]] =
    for {
      contracts <- contractStore.find(
        filterId = None,
        filterPackage = Some(pkg),
        filterTemplate = None,
        limit = Int.MaxValue,
      )
      cids = contracts.map(_.contractId)
      states <- fetchStates(cids)
    } yield {
      states.collectFirst { case (cid, StateChange(ActiveContractStore.Active, _)) =>
        cid
      }
    }
}

object InMemoryActiveContractStore {
  import ActiveContractStore.*

  /** A contract status change consists of the actual [[ActivenessChange]] (timestamp, request counter, and kind)
    * and the details. The [[com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail]]
    * determines whether the actual [[ActivenessChange]]
    * is a creation/archival or a transfer-in/out. In the store, at most one
    * [[com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail]]
    * may be associated with the same [[ActivenessChange]].
    */
  type IndividualChange = (ActivenessChange, ActivenessChangeDetail)
  object IndividualChange {
    def create(toc: TimeOfChange): IndividualChange =
      Activation(toc) -> ActivenessChangeDetail.Create
    def add(toc: TimeOfChange): IndividualChange =
      Activation(toc) -> ActivenessChangeDetail.Add
    def archive(toc: TimeOfChange): IndividualChange =
      Deactivation(toc) -> ActivenessChangeDetail.Archive
    def purge(toc: TimeOfChange): IndividualChange =
      Deactivation(toc) -> ActivenessChangeDetail.Purge
    def transferIn(
        toc: TimeOfChange,
        remoteDomainIdx: Int,
    ): IndividualChange =
      Activation(toc) -> ActivenessChangeDetail.TransferIn(remoteDomainIdx)

    def transferOut(
        toc: TimeOfChange,
        remoteDomainIdx: Int,
    ): IndividualChange =
      Deactivation(toc) -> ActivenessChangeDetail.TransferOut(remoteDomainIdx)
  }

  final case class ActivenessChange(toc: TimeOfChange, isActivation: Boolean) {
    def assertSameDetail(
        detail1: ActivenessChangeDetail,
        detail2: ActivenessChangeDetail,
        contractId: LfContractId,
    ): Checked[AcsError, AcsWarning, Unit] =
      if (detail1 == detail2) Checked.result(())
      else {
        val irregularity =
          if (isActivation) SimultaneousActivation(contractId, toc, detail1, detail2)
          else SimultaneousDeactivation(contractId, toc, detail1, detail2)
        Checked.continue(irregularity)
      }

  }
  object Activation {
    def apply(toc: TimeOfChange): ActivenessChange = ActivenessChange(toc, isActivation = true)
  }
  object Deactivation {
    def apply(toc: TimeOfChange): ActivenessChange = ActivenessChange(toc, isActivation = false)
  }

  object ActivenessChange {

    /** Intended order is by [[com.digitalasset.canton.participant.util.TimeOfChange]]
      * and then activations (creates/transfer-in) before deactivation,
      * but this is the reversed order because we want to iterate over the earlier events in the [[ChangeJournal]]
      */
    implicit val reverseOrderingForActivenessChange: Ordering[ActivenessChange] =
      Ordering
        .by[ActivenessChange, (TimeOfChange, Boolean)](change => (change.toc, !change.isActivation))
        .reverse

    implicit val reverseOrderForActivenessChange: Order[ActivenessChange] = Order.fromOrdering
  }

  type ChangeJournal = SortedMap[ActivenessChange, ActivenessChangeDetail]

  /** Journal of the changes to contract.
    *
    * @param changes The journal of changes that have been recorded for the contract.
    *                Must be ordered by [[ActivenessChange.reverseOrderingForActivenessChange]].
    */
  final case class ContractStatus private (
      changes: ChangeJournal
  ) {
    import IndividualChange.{archive, create}

    private def checkNewChangesJournal(
        contractId: LfContractId,
        toc: TimeOfChange,
        newChangesJournal: ChangeJournal,
    ): Checked[AcsError, AcsWarning, Unit] = {
      def checkedUnit = Checked.unit[AcsError, AcsWarning]

      val changes = newChangesJournal.toVector
        // we want earlier changes first
        .reverse
        .map { case ActivenessChange(toc, _) -> activenessChangeDetail =>
          (toc, activenessChangeDetail)
        }

      NonEmpty.from(changes).fold(checkedUnit) { changes =>
        NonEmptyChain
          .fromSeq(ActivationsDeactivationsConsistencyCheck(contractId, toc, changes))
          .fold(checkedUnit)(Checked.continues)
      }
    }

    private[InMemoryActiveContractStore] def addCreation(
        transferableContract: ActiveContractData,
        toc: TimeOfChange,
        isCreation: Boolean,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val contractId = transferableContract.contractId
      val change = if (isCreation) create(toc) else add(toc)

      for {
        nextChanges <- addIndividualChange(contractId, change)
        _ <- checkNewChangesJournal(contractId, toc, nextChanges)
      } yield this.copy(nextChanges)
    }

    private[InMemoryActiveContractStore] def addArchival(
        contractId: LfContractId,
        toc: TimeOfChange,
        isArchival: Boolean,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val change = if (isArchival) archive(toc) else purge(toc)

      for {
        nextChanges <- addIndividualChange(contractId, change)
        _ <- checkNewChangesJournal(contractId, toc, nextChanges)
      } yield this.copy(nextChanges)
    }

    private[InMemoryActiveContractStore] def addTransferIn(
        contractId: LfContractId,
        transfer: TimeOfChange,
        sourceDomainIdx: Int,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(
          contractId,
          IndividualChange.transferIn(transfer, sourceDomainIdx),
        )
        _ <- checkNewChangesJournal(contractId, transfer, nextChanges)
      } yield ContractStatus(nextChanges)

    private[InMemoryActiveContractStore] def addTransferOut(
        contractId: LfContractId,
        transfer: TimeOfChange,
        targetDomainIdx: Int,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(
          contractId,
          IndividualChange.transferOut(transfer, targetDomainIdx),
        )
        _ <- checkNewChangesJournal(contractId, transfer, nextChanges)

      } yield ContractStatus(nextChanges)

    private[this] def addIndividualChange(
        contractId: LfContractId,
        entry: IndividualChange,
    ): Checked[AcsError, AcsWarning, ChangeJournal] = {
      val (change, detail) = entry
      changes.get(change) match {
        case None => Checked.result(changes + (change -> detail))
        case Some(oldDetail) =>
          change.assertSameDetail(oldDetail, detail, contractId).map(_ => changes)
      }
    }

    /** If the contract is active right after the given `timestamp`,
      * returns the [[com.digitalasset.canton.data.CantonTimestamp]] of the latest creation or latest transfer-in.
      */
    def activeBy(timestamp: CantonTimestamp): Option[CantonTimestamp] = {
      val iter = changes.iteratorFrom(ContractStatus.searchByTimestamp(timestamp))
      if (!iter.hasNext) { None }
      else {
        val (change, _detail) = iter.next()
        if (change.isActivation) Some(change.toc.timestamp) else None
      }
    }

    /** If the contract is active right after the given `rc`,
      * returns the [[com.digitalasset.canton.RequestCounter]] of the latest creation or latest transfer-in.
      */
    def activeBy(rc: RequestCounter): Option[RequestCounter] =
      changes
        .filter { case (activenessChange, _) =>
          activenessChange.toc.rc <= rc
        }
        .toSeq
        .maxByOption { case (activenessChange, _) =>
          (activenessChange.toc, !activenessChange.isActivation)
        }
        .flatMap { case (change, _detail) =>
          Option.when(change.isActivation)((change.toc.rc))
        }

    def prune(beforeAndIncluding: CantonTimestamp): Option[ContractStatus] =
      changes.keys
        .filter(change => !change.isActivation && change.toc.timestamp <= beforeAndIncluding)
        .lastOption match {

        case Some(pruneToc) =>
          // The assumption here is that the only way in which activation and deactivation will share the same
          // timestamp is when the contract is 'passing through' in which case the deactivation is logically always
          // second.
          contractStatusFromChangeJournal(changes.filter(_._1.toc > pruneToc.toc))

        case None =>
          // Skipping changes without deactivations mimics behavior of db-based ACS store
          Some(this)
      }

    /** Returns a contract status that has all changes removed whose request counter is at least `criterion`. */
    def deleteSince(criterion: RequestCounter): ContractStatus = {
      val affected = changes.headOption.exists { case (change, _detail) =>
        change.toc.rc >= criterion
      }
      if (!affected) this
      else {
        val retainedChanges = changes.filter { case (change, _detail) => change.toc.rc < criterion }
        contractStatusFromChangeJournal(retainedChanges).getOrElse(ContractStatus.Nonexistent)
      }
    }

    private def contractStatusFromChangeJournal(journal: ChangeJournal): Option[ContractStatus] =
      if (journal.nonEmpty) {
        Some(ContractStatus(journal))
      } else None
  }

  object ContractStatus {
    private def apply(changes: ChangeJournal) =
      new ContractStatus(changes)

    val Nonexistent = new ContractStatus(SortedMap.empty)

    private def searchByTimestamp(timestamp: CantonTimestamp): ActivenessChange =
      Deactivation(TimeOfChange(RequestCounter.MaxValue, timestamp))
  }

}
