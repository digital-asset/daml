// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.Chain
import cats.kernel.Order
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.data.{ActiveContractData, ActiveContractsData}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractChange,
  ContractStore,
  StateChangeType,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{LfContractId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, TransferCounter, TransferCounterO}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicInteger
import scala.Ordered.orderingToOrdered
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Implements an [[ActiveContractStore!]] in memory. */
class InMemoryActiveContractStore(
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

  override def markContractsActive(
      contracts: Seq[(LfContractId, TransferCounterO)],
      toc: TimeOfChange,
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
              _.addCreation(transferableContract, activeContractsData.toc),
            )
          }
        })
    }
  }

  override def archiveContracts(archivals: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(show"Archiving contracts at $toc: $archivals")
      archivals.to(LazyList).traverse_ { case contractId =>
        updateTable(contractId, _.addArchival(contractId, toc))
      }
    })

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] =
    Future.successful {
      val snapshot = table.readOnlySnapshot()
      contractIds
        .to(LazyList)
        .mapFilter(contractId =>
          snapshot.get(contractId).flatMap(_.latestState.map(contractId -> _))
        )
        .toMap
    }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (CantonTimestamp, TransferCounterO)]] = Future.successful {
    val snapshot = SortedMap.newBuilder[LfContractId, (CantonTimestamp, TransferCounterO)]
    table.foreach { case (contractId, contractStatus) =>
      contractStatus.activeBy(timestamp).foreach { case (activationTimestamp, transferCounter) =>
        snapshot += (contractId -> (activationTimestamp, transferCounter))
      }
    }
    snapshot.result()
  }

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (RequestCounter, TransferCounterO)]] = Future.successful {
    val snapshot = SortedMap.newBuilder[LfContractId, (RequestCounter, TransferCounterO)]
    table.foreach { case (contractId, contractStatus) =>
      contractStatus.activeBy(rc).foreach { case (activationRc, transferCounter) =>
        snapshot += (contractId -> (activationRc, transferCounter))
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
          table.get(contractId).flatMap(_.activeBy(timestamp)).map { case (ts, _) =>
            contractId -> ts
          }
        )
        .toMap
    }

  override def bulkContractsTransferCounterSnapshot(
      contractIds: Set[LfContractId],
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, TransferCounterO]] = {
    logger.debug(
      s"Looking up transfer counters for contracts $contractIds up to but not including $requestCounter"
    )
    if (requestCounter == RequestCounter.MinValue)
      ErrorUtil.internalError(
        new IllegalArgumentException(
          s"The request counter $requestCounter should not be equal to ${RequestCounter.MinValue}"
        )
      )
    Future.successful {
      contractIds
        .to(LazyList)
        .mapFilter(contractId =>
          table
            .get(contractId)
            .flatMap(_.activeBy(requestCounter - 1))
            .map { case (_, transferCounter) =>
              contractId -> transferCounter
            }
        )
        .toMap
    }
  }

  override def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TransferCounterO, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(s"Transferring-in contracts: $transferIns")
      transferIns.to(LazyList).traverse_ { case (contractId, sourceDomain, transferCounter, toc) =>
        updateTable(contractId, _.addTransferIn(contractId, toc, sourceDomain, transferCounter))
      }
    })

  override def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TransferCounterO, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(Future.successful {
      logger.trace(s"Transferring-out contracts: $transferOuts")
      transferOuts.to(LazyList).traverse_ { case (contractId, targetDomain, transferCounter, toc) =>
        updateTable(contractId, _.addTransferOut(contractId, toc, targetDomain, transferCounter))
      }
    })

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

      // obtain the maximum creation timestamp per contract up to a certain rc
      val latestActivationTransferCounterPerCid
          : Map[(LfContractId, RequestCounter), TransferCounterO] =
        table.toList.flatMap { case (cid, status) =>
          // we only constrain here the upper bound timestamp, because we want to find the
          // transfer counter of archivals, which might have been activated earlier
          // than the lower bound
          /*
             TODO(i12904): Here we compute the maximum of the previous transfer counters;
              instead, we could retrieve the transfer counter of the latest activation
           */
          val filterToc = status.changes.filter { case (ch, _) => ch.toc <= toInclusive }
          filterToc
            .map { case (change, _) =>
              (
                (cid, change.toc.rc),
                filterToc
                  .collect {
                    case (ch, detail) if ch.isActivation && ch.toc.rc <= change.toc.rc =>
                      detail.transferCounter
                  }
                  .maxOption
                  .flatten,
              )
            }
        }.toMap

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
                StateChangeType(ContractChange.Created, activenessChangeDetail.transferCounter)
              else if (activenessChange.isActivation && activenessChangeDetail.isTransfer)
                StateChangeType(ContractChange.Assigned, activenessChangeDetail.transferCounter)
              else if (!activenessChange.isActivation && activenessChangeDetail.isTransfer)
                StateChangeType(ContractChange.Unassigned, activenessChangeDetail.transferCounter)
              else
                StateChangeType(
                  ContractChange.Archived,
                  latestActivationTransferCounterPerCid.getOrElse(
                    (coid, activenessChange.toc.rc),
                    activenessChangeDetail.transferCounter,
                  ),
                )
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
  ): Future[Option[(LfContractId)]] = {
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
      states.collectFirst { case (cid, StateChange(ActiveContractStore.Active(_), _)) =>
        cid
      }
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
    def create(toc: TimeOfChange, transferCounter: TransferCounterO): IndividualChange =
      Activation(toc) -> CreationArchivalDetail(transferCounter)
    def archive(toc: TimeOfChange): IndividualChange =
      Deactivation(toc) -> CreationArchivalDetail(None)
    def transferIn(
        toc: TimeOfChange,
        remoteDomain: DomainId,
        transferCounter: TransferCounterO,
    ): IndividualChange =
      Activation(toc) -> TransferDetails(remoteDomain, transferCounter)

    def transferOut(
        toc: TimeOfChange,
        remoteDomain: DomainId,
        transferCounter: TransferCounterO,
    ): IndividualChange =
      Deactivation(toc) -> TransferDetails(remoteDomain, transferCounter)
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
    * @param latestCreation Tracks the latest creation of the contract, if any.
    *                       Used to detect when another change happens before the contract was created.
    *                       If the contract is created several times, only the latest creation is tracked.
    *                       Transfer-ins do not count as creations.
    * @param earliestArchival Tracks the earliest archival of the contract, if any.
    *                         Used to detect when another change happens after the contract was archived.
    *                         If the contract is archived several times, only the earliest archive is tracked.
    *                         Transfer-outs do not count as archivals.
    */
  final case class ContractStatus private (
      changes: ChangeJournal,
      latestCreation: Option[TimeOfChange],
      earliestArchival: Option[TimeOfChange],
  ) {
    import IndividualChange.{archive, create}

    private[InMemoryActiveContractStore] def addCreation(
        transferableContract: ActiveContractData,
        creation: TimeOfChange,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val contractId = transferableContract.contractId

      val nextLatestCreation = latestCreation match {
        case None => Checked.result(Some(creation))
        case old @ Some(oldToc) if oldToc == creation => Checked.result(old)
        case old @ Some(oldToc) =>
          val newToc = if (creation > oldToc) Some(creation) else old
          Checked.continueWithResult(DoubleContractCreation(contractId, oldToc, creation), newToc)
      }

      // We don't report earlier changes if a double creation is detected.
      val earlierChanges =
        if (nextLatestCreation.successful)
          changesBefore(Activation(creation)).map(change =>
            ChangeBeforeCreation(contractId, creation, change.toc)
          )
        else List.empty

      for {
        nextChanges <- addIndividualChange(
          contractId,
          create(creation, transferableContract.transferCounter),
        )
        nextLatestCreation <- nextLatestCreation.appendNonaborts(Chain.fromSeq(earlierChanges))
        nextEarliestArchival <- checkTimestampAgainstArchival(contractId, creation)
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)
    }

    private[InMemoryActiveContractStore] def addArchival(
        contractId: LfContractId,
        archival: TimeOfChange,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val nextEarliestArchival = earliestArchival match {
        case None => Checked.result(Some(archival))
        case old @ Some(oldToc) if oldToc == archival => Checked.result(old)
        case old @ Some(oldToc) =>
          val newToc = if (archival < oldToc) Some(archival) else old
          Checked.continueWithResult(DoubleContractArchival(contractId, oldToc, archival), newToc)
      }
      // We don't report later changes if a double archival is detected.
      val laterChanges =
        if (nextEarliestArchival.successful)
          changesAfter(Deactivation(archival)).map(change =>
            ChangeAfterArchival(contractId, archival, change.toc)
          )
        else List.empty

      for {
        nextChanges <- addIndividualChange(contractId, archive(archival))
        nextLatestCreation <- checkTimestampAgainstCreation(contractId, archival)
        nextEarliestArchival <- nextEarliestArchival.appendNonaborts(Chain.fromSeq(laterChanges))
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)
    }

    private[InMemoryActiveContractStore] def addTransferIn(
        contractId: LfContractId,
        transfer: TimeOfChange,
        sourceDomain: SourceDomainId,
        transferCounter: TransferCounterO,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(
          contractId,
          IndividualChange.transferIn(transfer, sourceDomain.unwrap, transferCounter),
        )
        _ <- transferCounter.traverse_(
          checkTransferCounterIncreases(
            contractId,
            transfer,
            _,
            ActiveContractStore.TransferType.TransferIn,
          )
        )
        nextLatestCreation <- checkTimestampAgainstCreation(contractId, transfer)
        nextEarliestArchival <- checkTimestampAgainstArchival(contractId, transfer)
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)

    private[InMemoryActiveContractStore] def addTransferOut(
        contractId: LfContractId,
        transfer: TimeOfChange,
        targetDomain: TargetDomainId,
        transferCounter: TransferCounterO,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(
          contractId,
          IndividualChange.transferOut(transfer, targetDomain.unwrap, transferCounter),
        )
        _ <- transferCounter.traverse_(
          checkTransferCounterIncreases(
            contractId,
            transfer,
            _,
            ActiveContractStore.TransferType.TransferOut,
          )
        )
        nextLatestCreation <- checkTimestampAgainstCreation(contractId, transfer)
        nextEarliestArchival <- checkTimestampAgainstArchival(contractId, transfer)
      } yield ContractStatus(nextChanges, nextLatestCreation, nextEarliestArchival)

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

    private[this] def checkTimestampAgainstCreation(
        contractId: LfContractId,
        toc: TimeOfChange,
    ): Checked[AcsError, AcsWarning, Option[TimeOfChange]] =
      latestCreation match {
        case old @ Some(creation) if toc < creation =>
          Checked.continueWithResult(ChangeBeforeCreation(contractId, creation, toc), old)
        case old => Checked.result(old)
      }

    private[this] def checkTransferCounterIncreases(
        contractId: LfContractId,
        toc: TimeOfChange,
        transferCounter: TransferCounter,
        transferType: ActiveContractStore.TransferType,
    ): Checked[AcsError, AcsWarning, Unit] = {
      val isActivation = transferType match {
        case ActiveContractStore.TransferType.TransferIn => true
        case ActiveContractStore.TransferType.TransferOut => false
      }

      def toTransferCounterAtChangeInfo(
          change: ActivenessChange
      ): Option[TransferCounterAtChangeInfo] =
        changes.get(change).map { detail =>
          ActiveContractStore.TransferCounterAtChangeInfo(change.toc, detail.transferCounter)
        }

      val earliestChangeAfter =
        changesAfter(ActivenessChange(toc, isActivation)).lastOption
          .flatMap(toTransferCounterAtChangeInfo)
      val latestChangeBefore =
        changesBefore(ActivenessChange(toc, isActivation)).headOption
          .flatMap(toTransferCounterAtChangeInfo)
      for {
        _ <- ActiveContractStore.checkTransferCounterAgainstLatestBefore(
          contractId,
          toc,
          transferCounter,
          latestChangeBefore,
          transferType,
        )
        _ <- ActiveContractStore.checkTransferCounterAgainstEarliestAfter(
          contractId,
          toc,
          transferCounter,
          earliestChangeAfter,
          transferType,
        )
      } yield ()
    }

    private[this] def checkTimestampAgainstArchival(
        contractId: LfContractId,
        toc: TimeOfChange,
    ): Checked[AcsError, AcsWarning, Option[TimeOfChange]] =
      earliestArchival match {
        case old @ Some(archival) if toc > archival =>
          Checked.continueWithResult(ChangeAfterArchival(contractId, archival, toc), old)
        case old => Checked.result(old)
      }

    private[this] def changesAfter(bound: ActivenessChange): List[ActivenessChange] = {
      val laterChanges = mutable.SortedSet.newBuilder[ActivenessChange]
      val iterator = changes.keysIterator
      @tailrec def go(): Unit =
        if (iterator.hasNext) {
          val change = iterator.next()
          if (change < bound) {
            laterChanges += change
            go()
          }
        }
      go()
      laterChanges.result().toList
    }

    private[this] def changesBefore(bound: ActivenessChange): List[ActivenessChange] = {
      val laterChanges = mutable.SortedSet.newBuilder[ActivenessChange]
      val iterator = changes.keysIteratorFrom(bound)

      if (iterator.hasNext) {
        val change = iterator.next()
        // Skip the first change if it is the bound.
        if (change < bound) {
          laterChanges += change
        }
        iterator.foreach(change => laterChanges += change)
      }
      laterChanges.result().toList
    }

    /** If the contract is active right after the given `timestamp`,
      * returns the [[com.digitalasset.canton.data.CantonTimestamp]] of the latest creation or latest transfer-in.
      */
    def activeBy(timestamp: CantonTimestamp): Option[(CantonTimestamp, TransferCounterO)] = {
      val iter = changes.iteratorFrom(ContractStatus.searchByTimestamp(timestamp))
      if (!iter.hasNext) { None }
      else {
        val (change, detail) = iter.next()
        if (change.isActivation) Some((change.toc.timestamp, detail.transferCounter)) else None
      }
    }

    /** If the contract is active right after the given `rc`,
      * returns the [[com.digitalasset.canton.RequestCounter]] of the latest creation or latest transfer-in.
      */
    def activeBy(rc: RequestCounter): Option[(RequestCounter, TransferCounterO)] =
      changes
        .filter { case (activenessChange, _) =>
          activenessChange.toc.rc <= rc
        }
        .toSeq
        .maxByOption { case (activenessChange, _) =>
          (activenessChange.toc, !activenessChange.isActivation)
        }
        .flatMap { case (change, detail) =>
          Option.when(change.isActivation)((change.toc.rc, detail.transferCounter))
        }

    /** Returns the latest [[ActiveContractStore.ContractState]] if any */
    def latestState: Option[ContractState] = {
      changes.headOption.map { case (change, detail) =>
        val status =
          if (change.isActivation) Active(detail.transferCounter)
          else
            detail match {
              case TransferDetails(targetDomain, transferCounter) =>
                TransferredAway(TargetDomainId(targetDomain), transferCounter)
              case CreationArchivalDetail(transferCounter) => Archived
            }
        ContractState(status, change.toc)
      }
    }

    def prune(beforeAndIncluding: CantonTimestamp): Option[ContractStatus] = {
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

    private def contractStatusFromChangeJournal(journal: ChangeJournal): Option[ContractStatus] = {
      if (journal.nonEmpty) {
        val earliestArchival = journal.collect {
          case (change, detail) if !change.isActivation && !detail.isTransfer => change.toc
        }.lastOption
        val latestCreation = journal.collectFirst {
          case (change, details) if change.isActivation && !details.isTransfer => change.toc
        }
        Some(ContractStatus(journal, latestCreation, earliestArchival))
      } else None
    }
  }

  object ContractStatus {
    private def apply(
        changes: ChangeJournal,
        latestCreation: Option[TimeOfChange],
        earliestArchival: Option[TimeOfChange],
    ) =
      new ContractStatus(changes, latestCreation, earliestArchival)

    val Nonexistent = new ContractStatus(SortedMap.empty, None, None)

    private def searchByTimestamp(timestamp: CantonTimestamp): ActivenessChange =
      Deactivation(TimeOfChange(RequestCounter.MaxValue, timestamp))
  }

}
