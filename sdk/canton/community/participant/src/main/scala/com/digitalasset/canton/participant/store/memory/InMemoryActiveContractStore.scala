// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.NonEmptyChain
import cats.kernel.Order
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail.{
  Add,
  Archive,
  Assignment,
  Create,
  Purge,
  Unassignment,
}
import com.digitalasset.canton.participant.store.data.{ActiveContractData, ActiveContractsData}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicInteger
import scala.Ordered.orderingToOrdered
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Implements an [[ActiveContractStore!]] in memory. */
class InMemoryActiveContractStore(
    val indexedStringStore: IndexedStringStore,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ActiveContractStore
    with NamedLogging
    with InMemoryPrunableByTime {

  import ActiveContractStore.*
  import InMemoryActiveContractStore.*

  /** Invariant: Never maps to [[ContractStatus.Nonexistent]] */
  private[this] val table = TrieMap.empty[LfContractId, ContractStatus]

  override def markContractsCreatedOrAdded(
      contracts: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)],
      isCreation: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val activeContractsDataE = ActiveContractsData.create(contracts)
    activeContractsDataE match {
      case Left(errorMessage) =>
        CheckedT.abortT(ActiveContractsDataInvariantViolation(errorMessage))
      case Right(activeContractsData) =>
        CheckedT(FutureUnlessShutdown.pure {
          logger.trace(
            s"Creating contracts ${activeContractsData.contracts.toList}"
          )

          activeContractsData.asSeq.to(LazyList).traverse_ { activeContractData =>
            updateTable(
              activeContractData.contractId,
              _.addCreation(activeContractData, activeContractData.toc, isCreation = isCreation),
            )
          }
        })
    }
  }

  override def purgeOrArchiveContracts(
      contracts: Seq[(LfContractId, TimeOfChange)],
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    val operation = if (isArchival) "Archiving" else "Purging"

    CheckedT(FutureUnlessShutdown.pure {
      logger.trace(s"$operation contracts: $contracts")
      contracts.to(LazyList).traverse_ { case (contractId, toc) =>
        updateTable(contractId, _.addArchival(contractId, toc, isArchival = isArchival))
      }
    })
  }

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfContractId, ContractState]] = {
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[ContractState]] =
    changes.headOption.traverse { case (change, detail) =>
      val statusF = detail match {
        case ActivenessChangeDetail.Create(reassignmentCounter) =>
          FutureUnlessShutdown.pure(Active(reassignmentCounter))
        case ActivenessChangeDetail.Archive => FutureUnlessShutdown.pure(Archived)

        case assignment: ActivenessChangeDetail.Assignment =>
          FutureUnlessShutdown.pure(Active(assignment.reassignmentCounter))
        case unassignment: ActivenessChangeDetail.Unassignment =>
          synchronizerIdFromIdx(unassignment.remoteSynchronizerIdx).map(synchronizerId =>
            ReassignedAway(Target(synchronizerId), unassignment.reassignmentCounter)
          )

        case ActivenessChangeDetail.Purge => FutureUnlessShutdown.pure(Purged)
        case ActivenessChangeDetail.Add(reassignmentCounter) =>
          FutureUnlessShutdown.pure(Active(reassignmentCounter))

      }

      statusF.map(ContractState(_, change.toc))
    }

  override def snapshot(timeOfChange: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, (TimeOfChange, ReassignmentCounter)]] =
    FutureUnlessShutdown.pure {
      val snapshot = SortedMap.newBuilder[LfContractId, (TimeOfChange, ReassignmentCounter)]
      table.foreach { case (contractId, contractStatus) =>
        contractStatus.activeBy(timeOfChange).foreach {
          case (activationTimestamp, reassignmentCounter) =>
            snapshot += (contractId -> (activationTimestamp, reassignmentCounter))
        }
      }
      snapshot.result()
    }

  override def activenessOf(contracts: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]] =
    FutureUnlessShutdown.pure {
      val snapshot =
        SortedMap.newBuilder[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]
      if (contracts.nonEmpty)
        table.view.filterKeys(contracts.toSet).foreach { case (contractId, contractStatus) =>
          snapshot += (contractId ->
            contractStatus.changes.map { case (activenessChange, state) =>
              (activenessChange.toc.timestamp, state)
            }.toSeq)
        }
      snapshot.result()
    }

  override def contractSnapshot(contractIds: Set[LfContractId], timeOfChange: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, TimeOfChange]] =
    FutureUnlessShutdown.pure {
      contractIds
        .to(LazyList)
        .mapFilter(contractId =>
          table.get(contractId).flatMap(_.activeBy(timeOfChange)).map { case (toc, _) =>
            contractId -> toc
          }
        )
        .toMap
    }

  override def contractsReassignmentCounterSnapshotBefore(
      contractIds: Set[LfContractId],
      timestampExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ReassignmentCounter]] = {
    logger.debug(
      s"Looking up reassignment counters for contracts $contractIds up to but not including $timestampExclusive"
    )
    FutureUnlessShutdown.pure {
      contractIds
        .to(LazyList)
        .map(contractId =>
          table
            .get(contractId)
            .flatMap(_.activeBy(TimeOfChange.immediatePredecessor(timestampExclusive)))
            .fold(
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Archived non-transient contract $contractId should have been active in the ACS"
                )
              )
            ) { case (_, reassignmentCounter) =>
              contractId -> reassignmentCounter
            }
        )
        .toMap
    }
  }

  private def prepareReassignments(
      reassignments: Seq[
        (LfContractId, ReassignmentTag[SynchronizerId], ReassignmentCounter, TimeOfChange)
      ]
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Seq[
    (LfContractId, Int, ReassignmentCounter, TimeOfChange)
  ]] = {
    val synchronizers = reassignments.map { case (_, synchronizer, _, _) =>
      synchronizer.unwrap
    }.distinct
    type PreparedReassignment = (LfContractId, Int, ReassignmentCounter, TimeOfChange)

    for {
      synchronizerIndices <- getSynchronizerIndices(synchronizers)

      preparedReassignmentsE = MonadUtil.sequentialTraverse(
        reassignments
      ) { case (cid, remoteSynchronizer, reassignmentCounter, toc) =>
        synchronizerIndices
          .get(remoteSynchronizer.unwrap)
          .toRight[AcsError](UnableToFindIndex(remoteSynchronizer.unwrap))
          .map(synchronizerIdx => (cid, synchronizerIdx.index, reassignmentCounter, toc))
      }

      preparedReassignments <- CheckedT.fromChecked(
        Checked.fromEither(preparedReassignmentsE)
      ): CheckedT[
        FutureUnlessShutdown,
        AcsError,
        AcsWarning,
        Seq[PreparedReassignment],
      ]
    } yield preparedReassignments
  }

  override def assignContracts(
      assignments: Seq[(LfContractId, Source[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    logger.trace(s"Assigning contracts: $assignments")

    for {
      preparedReassignments <- prepareReassignments(assignments)
      _ <- CheckedT(FutureUnlessShutdown.pure(preparedReassignments.to(LazyList).traverse_ {
        case (contractId, sourceSynchronizer, reassignmentCounter, toc) =>
          updateTable(
            contractId,
            _.addAssignment(contractId, toc, sourceSynchronizer, reassignmentCounter),
          )
      }))
    } yield ()
  }

  override def unassignContracts(
      unassignments: Seq[(LfContractId, Target[SynchronizerId], ReassignmentCounter, TimeOfChange)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] = {
    logger.trace(s"Unassigning contracts: $unassignments")

    for {
      preparedReassignments <- prepareReassignments(unassignments)
      _ <- CheckedT(FutureUnlessShutdown.pure(preparedReassignments.to(LazyList).traverse_ {
        case (contractId, sourceSynchronizer, reassignmentCounter, toc) =>
          updateTable(
            contractId,
            _.addUnassignment(contractId, toc, sourceSynchronizer, reassignmentCounter),
          )
      }))
    } yield ()
  }

  override def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
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
    FutureUnlessShutdown.pure(counter.get())
  }

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    table.clear()
    FutureUnlessShutdown.unit
  }

  override def deleteSince(
      criterion: TimeOfChange
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(table.values.count {
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
  ): FutureUnlessShutdown[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    FutureUnlessShutdown.pure {
      ErrorUtil.requireArgument(
        fromExclusive <= toInclusive,
        s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
      )

      // obtain the maximum reassignment counter per contract up to a certain rc
      val latestActivationReassignmentCounterPerCid
          : Map[(LfContractId, TimeOfChange), ReassignmentCounter] =
        table.toList.flatMap { case (cid, status) =>
          // we only constrain here the upper bound timestamp, because we want to find the
          // reassignment counter of archivals, which might have been activated earlier
          // than the lower bound
          /*
             TODO(i12904): Here we compute the maximum of the previous reassignment counters;
              instead, we could retrieve the reassignment counter of the latest activation
           */
          val filterToc = status.changes.filter { case (ch, _) => ch.toc <= toInclusive }
          filterToc
            .map { case (change, _) =>
              (
                (cid, change.toc),
                filterToc
                  .collect {
                    case (ch, detail) if ch.isActivation && ch.toc <= change.toc =>
                      detail.reassignmentCounterO
                  }
                  .maxOption
                  .flatten,
              )
            }
            .mapFilter(identity)
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
            val stateChange = activenessChangeDetail match {
              case change: ActivenessChangeDetail.HasReassignmentCounter => change.toStateChangeType

              case ActivenessChangeDetail.Archive | ActivenessChangeDetail.Purge =>
                val reassignmentCounter = latestActivationReassignmentCounterPerCid.getOrElse(
                  (coid, activenessChange.toc),
                  throw new IllegalStateException(
                    s"Unable to find reassignment counter for $coid at ${activenessChange.toc}"
                  ),
                )

                StateChangeType(ContractChange.Archived, reassignmentCounter)
            }

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
  ): FutureUnlessShutdown[Option[(LfContractId)]] =
    for {
      contracts <- contractStore.find(
        exactId = None,
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

object InMemoryActiveContractStore {
  import ActiveContractStore.*

  /** A contract status change consists of the actual [[ActivenessChange]] (timestamp, request
    * counter, and kind) and the details. The
    * [[com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail]]
    * determines whether the actual [[ActivenessChange]] is a creation/archival or a
    * assignment/unassignment. In the store, at most one
    * [[com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail]] may
    * be associated with the same [[ActivenessChange]].
    */
  type IndividualChange = (ActivenessChange, ActivenessChangeDetail)
  object IndividualChange {
    def create(toc: TimeOfChange, reassignmentCounter: ReassignmentCounter): IndividualChange =
      Activation(toc) -> ActivenessChangeDetail.Create(reassignmentCounter)
    def add(toc: TimeOfChange, reassignmentCounter: ReassignmentCounter): IndividualChange =
      Activation(toc) -> ActivenessChangeDetail.Add(reassignmentCounter)
    def archive(toc: TimeOfChange): IndividualChange =
      Deactivation(toc) -> ActivenessChangeDetail.Archive
    def purge(toc: TimeOfChange): IndividualChange =
      Deactivation(toc) -> ActivenessChangeDetail.Purge
    def assign(
        toc: TimeOfChange,
        remoteSynchronizerIdx: Int,
        reassignmentCounter: ReassignmentCounter,
    ): IndividualChange =
      Activation(toc) -> ActivenessChangeDetail.Assignment(
        reassignmentCounter,
        remoteSynchronizerIdx,
      )

    def unassign(
        toc: TimeOfChange,
        remoteSynchronizerIdx: Int,
        reassignmentCounter: ReassignmentCounter,
    ): IndividualChange =
      Deactivation(toc) -> ActivenessChangeDetail.Unassignment(
        reassignmentCounter,
        remoteSynchronizerIdx,
      )
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

    /** Intended order is by [[com.digitalasset.canton.participant.util.TimeOfChange]] and then
      * activations (creates/assignments) before deactivation, but this is the reversed order
      * because we want to iterate over the earlier events in the [[ChangeJournal]]
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
    * @param changes
    *   The journal of changes that have been recorded for the contract. Must be ordered by
    *   [[ActivenessChange.reverseOrderingForActivenessChange]].
    */
  final case class ContractStatus private (changes: ChangeJournal) {
    import IndividualChange.{add, archive, create, purge}

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
        activeContractData: ActiveContractData,
        toc: TimeOfChange,
        isCreation: Boolean,
    ): Checked[AcsError, AcsWarning, ContractStatus] = {
      val contractId = activeContractData.contractId
      val change =
        if (isCreation) create(toc, activeContractData.reassignmentCounter)
        else add(toc, activeContractData.reassignmentCounter)

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

    private[InMemoryActiveContractStore] def addAssignment(
        contractId: LfContractId,
        toc: TimeOfChange,
        sourceSynchronizerIdx: Int,
        reassignmentCounter: ReassignmentCounter,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(
          contractId,
          IndividualChange.assign(toc, sourceSynchronizerIdx, reassignmentCounter),
        )
        _ <- checkReassignmentCounterIncreases(
          contractId,
          toc,
          reassignmentCounter,
          ActiveContractStore.ReassignmentType.Assignment,
        )
        _ <- checkNewChangesJournal(contractId, toc, nextChanges)
      } yield this.copy(nextChanges)

    private[InMemoryActiveContractStore] def addUnassignment(
        contractId: LfContractId,
        toc: TimeOfChange,
        targetSynchronizerIdx: Int,
        reassignmentCounter: ReassignmentCounter,
    ): Checked[AcsError, AcsWarning, ContractStatus] =
      for {
        nextChanges <- addIndividualChange(
          contractId,
          IndividualChange.unassign(toc, targetSynchronizerIdx, reassignmentCounter),
        )
        _ <-
          checkReassignmentCounterIncreases(
            contractId,
            toc,
            reassignmentCounter,
            ActiveContractStore.ReassignmentType.Unassignment,
          )
        _ <- checkNewChangesJournal(contractId, toc, nextChanges)
      } yield this.copy(nextChanges)

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

    private[this] def checkReassignmentCounterIncreases(
        contractId: LfContractId,
        toc: TimeOfChange,
        reassignmentCounter: ReassignmentCounter,
        reassignmentType: ActiveContractStore.ReassignmentType,
    ): Checked[AcsError, AcsWarning, Unit] = {
      val isActivation = reassignmentType match {
        case ActiveContractStore.ReassignmentType.Assignment => true
        case ActiveContractStore.ReassignmentType.Unassignment => false
      }

      def toReassignmentCounterAtChangeInfo(
          change: ActivenessChange
      ): Option[ReassignmentCounterAtChangeInfo] =
        changes.get(change).map { detail =>
          ActiveContractStore
            .ReassignmentCounterAtChangeInfo(change.toc, detail.reassignmentCounterO)
        }

      val earliestChangeAfter =
        changesAfter(ActivenessChange(toc, isActivation)).lastOption
          .flatMap(toReassignmentCounterAtChangeInfo)
      val latestChangeBefore =
        changesBefore(ActivenessChange(toc, isActivation)).headOption
          .flatMap(toReassignmentCounterAtChangeInfo)
      for {
        _ <- ActiveContractStore.checkReassignmentCounterAgainstLatestBefore(
          contractId,
          toc,
          reassignmentCounter,
          latestChangeBefore,
        )
        _ <- ActiveContractStore.checkReassignmentCounterAgainstEarliestAfter(
          contractId,
          toc,
          reassignmentCounter,
          earliestChangeAfter,
          reassignmentType,
        )
      } yield ()
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

    /** If the contract is active right after the given `timestamp`, returns the
      * [[com.digitalasset.canton.data.CantonTimestamp]] of the latest creation or latest
      * assignment.
      */
    def activeBy(toc: TimeOfChange): Option[(TimeOfChange, ReassignmentCounter)] = {
      val iter = changes.iteratorFrom(ContractStatus.searchByTimestamp(toc))
      if (!iter.hasNext) { None }
      else {
        val (change, detail) = iter.next()

        def changeFor(reassignmentCounter: ReassignmentCounter) = Some(
          (change.toc, reassignmentCounter)
        )

        detail match {
          case Assignment(reassignmentCounter, _) => changeFor(reassignmentCounter)
          case Create(reassignmentCounter) => changeFor(reassignmentCounter)
          case Add(reassignmentCounter) => changeFor(reassignmentCounter)
          case Archive | _: Unassignment | Purge => None
        }
      }
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

    /** Returns a contract status that has all changes removed whose time of change is at least
      * `criterion`.
      */
    def deleteSince(criterion: TimeOfChange): ContractStatus = {
      val affected = changes.headOption.exists { case (change, _detail) =>
        change.toc >= criterion
      }
      if (!affected) this
      else {
        val retainedChanges = changes.filter { case (change, _detail) => change.toc < criterion }
        contractStatusFromChangeJournal(retainedChanges).getOrElse(ContractStatus.Nonexistent)
      }
    }

    private def contractStatusFromChangeJournal(journal: ChangeJournal): Option[ContractStatus] =
      Option.when(journal.nonEmpty)(ContractStatus(journal))
  }

  object ContractStatus {
    private def apply(changes: ChangeJournal) = new ContractStatus(changes)

    val Nonexistent = new ContractStatus(SortedMap.empty)

    private def searchByTimestamp(toc: TimeOfChange): ActivenessChange =
      Deactivation(toc)
  }
}
