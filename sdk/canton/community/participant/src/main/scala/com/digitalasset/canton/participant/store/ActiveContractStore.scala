// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String36}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{ReassignmentCounter, RequestCounter}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** <p>The active contract store (ACS) stores for every contract ID
  * whether it is inexistent, [[ActiveContractStore.Active]],
  * [[ActiveContractStore.Archived]], or [[ActiveContractStore.ReassignedAway]],
  * along with the timestamp of the latest change.
  * Every change is associated with the timestamp and request counter of the request that triggered the change.
  * The changes are ordered first by timestamp, then by request counter, and finally by change type (activations before deactivations).
  * Implementations must be thread-safe.
  * Updates must be idempotent.</p>
  *
  * <p>Creations, reassignments, and archivals can be mixed arbitrarily.
  * A contract may be assigned and unassigned several times during its lifetime.
  * It becomes active with every assignment and reassigned away with every unassignment.
  * If the ACS detects irregularities, the change method reports them.</p>
  *
  * <p>These methods are supposed to be called by the `ConflictDetector` only,
  * which coordinates the request journal updates and the updates to the ACS.</p>
  *
  * <p>Updates may be written asynchronously.
  * Every implementation determines an order over all the changes and queries to the ACS.
  * Each individual change must be applied atomically and the result is determined with respect to this order.
  * This order need not be consistent with the order of the calls, though.
  * However, the following is guaranteed:
  * If the future returned by a call completes and observing the completion happens before another call,
  * then all changes of the former call must be ordered before all changes of the later call.</p>
  *
  * <p>Bulk methods like [[ActiveContractStore.markContractsCreated]] and [[ActiveContractStore.archiveContracts]]
  * generate one individual change for each contract.
  * So their changes may be interleaved with other calls.</p>
  *
  * @see ActiveContractSnapshot for the ACS snapshot interface
  */
trait ActiveContractStore
    extends ActiveContractSnapshot
    with ConflictDetectionStore[LfContractId, ActiveContractStore.Status] {
  import ActiveContractStore.*

  override protected def kind: String = "active contract journal entries"
  private[store] def indexedStringStore: IndexedStringStore

  /** Marks the given contracts as active from `timestamp` (inclusive) onwards.
    *
    * @param contracts The contracts represented as a tuple of contract id and reassignment counter
    * @param toc The time of change consisting of
    *            <ul>
    *              <li>The request counter of the confirmation request that created the contracts</li>
    *              <li>The timestamp of the confirmation request that created the contracts.</li>
    *            </ul>
    * @return The future completes when all contract states have been updated.
    *         The following irregularities are reported for each contract:
    *         <ul>
    *           <li>[[ActiveContractStore.DoubleContractCreation]] if the contract is created a second time.</li>
    *           <li>[[ActiveContractStore.SimultaneousActivation]] if the contract is assigned at the same time
    *             or has been created by a different request at the same time.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] for every change that occurs before the creation timestamp.
    *             This is reported only if no [[ActiveContractStore.DoubleContractCreation]] is reported.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] if this creation is later than the earliest archival of the contract.</li>
    *         </ul>
    */
  def markContractsCreated(contracts: Seq[(LfContractId, ReassignmentCounter)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsCreatedOrAdded(
      contracts.map { case (cid, tc) => (cid, tc, toc) },
      isCreation = true,
    )

  /** Shorthand for `markContractsCreated(Seq(contract), toc)` */
  def markContractCreated(contract: (LfContractId, ReassignmentCounter), toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val (cid, tc) = contract
    markContractsCreatedOrAdded(Seq((cid, tc, toc)), isCreation = true)
  }

  /** Shorthand for `markContractAdded(Seq(contract), toc)` */
  def markContractAdded(contract: (LfContractId, ReassignmentCounter, TimeOfChange))(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsAdded(Seq(contract))

  /** Marks the given contracts as active from `timestamp` (inclusive) onwards.
    *
    * Unlike creation, add can be done several times in the life of a contract.
    * It is intended to use from the repair service.
    */
  def markContractsAdded(contracts: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsCreatedOrAdded(contracts, isCreation = false)

  def markContractsCreatedOrAdded(
      contracts: Seq[(LfContractId, ReassignmentCounter, TimeOfChange)],
      isCreation: Boolean, // true if create, false if add
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit]

  /** Marks the given contracts as archived from `toc`'s timestamp (inclusive) onwards.
    *
    * @param contractIds The contract IDs of the contracts to be archived
    *                    Note: this method should not take as parameter the reassignment counter
    *                    for the archived contract IDs, because one cannot know the correct
    *                    reassignment counter for an archival at request finalization time, which
    *                    is when this method is called.
    *                    The [[com.digitalasset.canton.participant.event.RecordOrderPublisher]]
    *                    determines the correct reassignment counter for an archival only when
    *                    all requests preceding the archival (i.e., with a lower request
    *                    counter than the archival transaction) were processed. Therefore, we determine
    *                    the reassignment counter for archivals in the
    *                    [[com.digitalasset.canton.participant.event.RecordOrderPublisher]], when
    *                    the record order publisher triggers an acs change event.
    * @param toc The time of change consisting of
    *            <ul>
    *              <li>The request counter of the confirmation request that archives the contracts.</li>
    *              <li>The timestamp on the confirmation request that archives the contracts.</li>
    *            </ul>
    * @return The future completes when all contract states have been updated.
    *         The following irregularities are reported for each contract:
    *         <ul>
    *           <li>[[ActiveContractStore.DoubleContractArchival]] if the contract is archived a second time.</li>
    *           <li>[[ActiveContractStore.SimultaneousDeactivation]] if the contract is unassigned at the same time
    *             or has been archived by a different request at the same time.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] for every change that occurs after the archival timestamp.
    *             This is reported only if no [[ActiveContractStore.DoubleContractArchival]] is reported.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] if this archival is earlier than the latest creation of the contract.</li>
    *         </ul>
    */
  def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    purgeOrArchiveContracts(contractIds.map((_, toc)), isArchival = true)

  /** Shorthand for `archiveContracts(Seq(cid), toc)` */
  def archiveContract(cid: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    archiveContracts(Seq(cid), toc)

  /** Shorthand for `purgeContracts(Seq(cid), toc)` */
  def purgeContract(cid: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    purgeOrArchiveContracts(Seq((cid, toc)), isArchival = false)

  /** Marks the given contracts as inactive from `timestamp` (inclusive) onwards.
    *
    * Unlike archival, purge can be done several times in the life of a contract.
    * It is intended to use from the repair service.
    */
  def purgeContracts(contractIds: Seq[(LfContractId, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    purgeOrArchiveContracts(contractIds, isArchival = false)

  /** Depending on the `isArchival`, will archive (effect of a Daml transaction) or purge (repair service)
    */
  def purgeOrArchiveContracts(
      contracts: Seq[(LfContractId, TimeOfChange)],
      isArchival: Boolean,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit]

  /** Returns the latest [[com.digitalasset.canton.participant.store.ActiveContractStore.Status]]
    * for the given contract IDs along with its [[com.digitalasset.canton.participant.util.TimeOfChange]].
    *
    * This method is used by the protocol processors for conflict detection.
    * In-flight transactions may have changesets not yet written to the ACS datastore.
    * Since only the `ConflictDetector` tracks in-flight changesets,
    * this method cannot be used as a source of valid data to other components.
    *
    * If a contract is created or assigned and archived or unassigned at the same [[com.digitalasset.canton.participant.util.TimeOfChange]],
    * the contract is [[ActiveContractStore.Archived]] or [[ActiveContractStore.ReassignedAway]].
    * A contract cannot be archived and unassigned at the same timestamp.
    *
    * @return The map from contracts in `contractIds` in the store to their latest state.
    *         Nonexistent contracts are excluded from the map.
    * @see ActiveContractSnapshot!.snapshot
    */
  def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfContractId, ContractState]]

  /** Marks the given contracts as assigned from `toc`'s timestamp (inclusive) onwards.
    *
    * @param assignments The contract IDs to assign, each with its source domain, reassignment counter and time of change.
    * @return The future completes when the contract states have been updated.
    *         The following irregularities are reported:
    *         <ul>
    *           <li>[[ActiveContractStore.SimultaneousActivation]] if an assignment from another domain or a creation
    *             has been added with the same timestamp.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] if this timestamp is after the earliest archival of the contract.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] if this timestamp is before the latest creation of the contract.</li>
    *           <li>[[ActiveContractStore.ReassignmentCounterShouldIncrease]] if the reassignment counter does not increase monotonically.</li>
    *         </ul>
    */
  def assignContracts(
      assignments: Seq[(LfContractId, Source[DomainId], ReassignmentCounter, TimeOfChange)]
  )(implicit traceContext: TraceContext): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit]

  def assignContract(
      contractId: LfContractId,
      toc: TimeOfChange,
      sourceDomain: Source[DomainId],
      reassignmentCounter: ReassignmentCounter,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    assignContracts(Seq((contractId, sourceDomain, reassignmentCounter, toc)))

  /** Marks the given contracts as [[ActiveContractStore.ReassignedAway]] from `toc`'s timestamp (inclusive) onwards.
    *
    * @param unassignments The contract IDs to unassign, each with its target domain, reassignment counter and time of change.
    * @return The future completes when the contract state has been updated.
    *         The following irregularities are reported:
    *         <ul>
    *           <li>[[ActiveContractStore.SimultaneousDeactivation]] if an unassignment to another domain or a creation
    *             has been added with the same timestamp.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] if this timestamp is after the earliest archival of the contract.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] if this timestamp is before the latest creation of the contract.</li>
    *           <li>[[ActiveContractStore.ReassignmentCounterShouldIncrease]] if the reassignment counter does not increase monotonically.</li>
    *         </ul>
    */
  def unassignContracts(
      unassignments: Seq[(LfContractId, Target[DomainId], ReassignmentCounter, TimeOfChange)]
  )(implicit traceContext: TraceContext): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit]

  def unassignContracts(
      contractId: LfContractId,
      toc: TimeOfChange,
      targetDomain: Target[DomainId],
      reassignmentCounter: ReassignmentCounter,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Unit] =
    unassignContracts(Seq((contractId, targetDomain, reassignmentCounter, toc)))

  /** Deletes all entries about archived contracts whose status hasn't changed after the timestamp.
    *
    * The caller must ensure that the given timestamp is at most the one of the clean cursor in the
    * [[com.digitalasset.canton.participant.protocol.RequestJournal]]
    */
  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Future[Int]

  /** Deletes all activeness changes from requests whose request counter is at least the given one.
    * This method must not be called concurrently with creating, archiving, or reassigning contracts.
    *
    * Therefore, this method need not be linearizable w.r.t. creating, archiving, or reassigning contracts.
    * For example, if a request `rc1` creates a contract `c` and another request `rc2` archives it
    * while [[deleteSince]] is running for some `rc <= rc1, rc2`, then there are no guarantees
    * which of the effects of `rc1` and `rc2` remain. For example, `c` could end up being inexistent, active, or
    * archived but never created, even if the writes for `rc1` and `rc2` are successful.
    */
  def deleteSince(criterion: RequestCounter)(implicit traceContext: TraceContext): Future[Unit]

  /** Returns the total number of contracts whose states are stored at the given timestamp.
    *
    * To get a consistent snapshot, the caller must ensure that the timestamp specifies a time that
    * is not in the future, i.e., not after the timestamp of the clean cursor in the
    * [[com.digitalasset.canton.participant.protocol.RequestJournal]]
    * Note that the result may change between two invocations if [[prune]] is called in the meantime.
    */
  @VisibleForTesting
  private[participant] def contractCount(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int]

  protected def domainIdFromIdxFUS(
      idx: Int
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[DomainId] =
    IndexedDomain
      .fromDbIndexOT("par_active_contracts remote domain index", indexedStringStore)(idx)
      .map(_.domainId)
      .value
      .flatMap {
        case Some(domainId) => FutureUnlessShutdown.pure(domainId)
        case None =>
          FutureUnlessShutdown.failed(
            new RuntimeException(s"Unable to find domain ID for domain with index $idx")
          )
      }

  protected def getDomainIndices(
      domains: Seq[DomainId]
  ): CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, Map[DomainId, IndexedDomain]] =
    CheckedT.result(
      domains
        .parTraverse { domainId =>
          IndexedDomain.indexed(indexedStringStore)(domainId).map(domainId -> _)
        }
        .map(_.toMap)
    )
}

object ActiveContractStore {

  type ContractState = StateChange[Status]
  val ContractState: StateChange.type = StateChange

  sealed abstract class ChangeType(val name: String)

  object ChangeType {
    case object Activation extends ChangeType("activation")
    case object Deactivation extends ChangeType("deactivation")

    implicit val setParameterChangeType: SetParameter[ChangeType] = (v, pp) => pp >> v.name
    implicit val getResultChangeType: GetResult[ChangeType] = GetResult(r =>
      r.nextString() match {
        case ChangeType.Activation.name => ChangeType.Activation
        case ChangeType.Deactivation.name => ChangeType.Deactivation
        case unknown => throw new DbDeserializationException(s"Unknown change type [$unknown]")
      }
    )
  }

  sealed trait ActivenessChangeDetail extends Product with Serializable with PrettyPrinting {
    def name: LengthLimitedString

    def reassignmentCounterO: Option[ReassignmentCounter]
    def remoteDomainIdxO: Option[Int]

    def changeType: ChangeType
    def contractChange: ContractChange

    def isReassignment: Boolean
  }

  object ActivenessChangeDetail {
    val create: String36 = String36.tryCreate("create")
    val archive: String36 = String36.tryCreate("archive")
    val add: String36 = String36.tryCreate("add")
    val purge: String36 = String36.tryCreate("purge")
    val assign: String36 = String36.tryCreate("assign")
    val unassignment: String36 = String36.tryCreate("unassign")

    sealed trait HasReassignmentCounter extends ActivenessChangeDetail {
      def reassignmentCounter: ReassignmentCounter
      override def reassignmentCounterO: Option[ReassignmentCounter] = Some(reassignmentCounter)
      def toStateChangeType: StateChangeType = StateChangeType(contractChange, reassignmentCounter)
    }

    sealed trait ReassignmentChangeDetail extends HasReassignmentCounter {
      def toReassignmentType: ActiveContractStore.ReassignmentType
      def remoteDomainIdx: Int
      override def remoteDomainIdxO: Option[Int] = Some(remoteDomainIdx)

      override def isReassignment: Boolean = true
    }

    final case class Create(reassignmentCounter: ReassignmentCounter)
        extends HasReassignmentCounter {
      override val name = ActivenessChangeDetail.create
      override def reassignmentCounterO: Option[ReassignmentCounter] = Some(reassignmentCounter)

      override def remoteDomainIdxO: Option[Int] = None

      override def changeType: ChangeType = ChangeType.Activation

      override def contractChange: ContractChange = ContractChange.Created

      override def isReassignment: Boolean = false
      override protected def pretty: Pretty[Create.this.type] = prettyOfClass(
        param("reassignment counter", _.reassignmentCounter)
      )
    }

    final case class Add(reassignmentCounter: ReassignmentCounter) extends HasReassignmentCounter {
      override val name = ActivenessChangeDetail.add
      override def remoteDomainIdxO: Option[Int] = None

      override def changeType: ChangeType = ChangeType.Activation

      override def contractChange: ContractChange = ContractChange.Created

      override def isReassignment: Boolean = false

      override protected def pretty: Pretty[Add.this.type] = prettyOfClass(
        param("reassignment counter", _.reassignmentCounter)
      )
    }

    /** The reassignment counter for archivals stored in the acs is always None, because we cannot
      * determine the correct reassignment counter when the contract is archived.
      * We only determine the reassignment counter later, when the record order publisher triggers the
      * computation of acs commitments, but we never store it in the acs.
      */
    case object Archive extends ActivenessChangeDetail {
      override val name = ActivenessChangeDetail.archive
      override def reassignmentCounterO: Option[ReassignmentCounter] = None
      override def remoteDomainIdxO: Option[Int] = None
      override def changeType: ChangeType = ChangeType.Deactivation

      override def contractChange: ContractChange = ContractChange.Archived

      override def isReassignment: Boolean = false

      override protected def pretty: Pretty[Archive.this.type] = prettyOfObject[Archive.this.type]
    }

    case object Purge extends ActivenessChangeDetail {
      override val name = ActivenessChangeDetail.purge
      override def reassignmentCounterO: Option[ReassignmentCounter] = None

      override def remoteDomainIdxO: Option[Int] = None

      override def changeType: ChangeType = ChangeType.Deactivation

      override def contractChange: ContractChange = ContractChange.Purged

      override def isReassignment: Boolean = false

      override protected def pretty: Pretty[Purge.this.type] = prettyOfObject[Purge.this.type]
    }

    final case class Assignment(reassignmentCounter: ReassignmentCounter, remoteDomainIdx: Int)
        extends ReassignmentChangeDetail {
      override val name = ActivenessChangeDetail.assign

      override def changeType: ChangeType = ChangeType.Activation
      override def toReassignmentType: ActiveContractStore.ReassignmentType =
        ActiveContractStore.ReassignmentType.Assignment

      override def contractChange: ContractChange = ContractChange.Assigned

      override protected def pretty: Pretty[Assignment.this.type] = prettyOfClass(
        param("contract change", _.contractChange),
        param("reassignment counter", _.reassignmentCounter),
        param("remote domain index", _.remoteDomainIdx),
      )
    }

    final case class Unassignment(reassignmentCounter: ReassignmentCounter, remoteDomainIdx: Int)
        extends ReassignmentChangeDetail {
      override val name = ActivenessChangeDetail.unassignment

      override def changeType: ChangeType = ChangeType.Deactivation
      override def toReassignmentType: ActiveContractStore.ReassignmentType =
        ActiveContractStore.ReassignmentType.Unassignment

      override def contractChange: ContractChange = ContractChange.Unassigned

      override protected def pretty: Pretty[Unassignment.this.type] = prettyOfClass(
        param("contract change", _.contractChange),
        param("reassignment counter", _.reassignmentCounter),
        param("remote domain index", _.remoteDomainIdx),
      )
    }

    implicit val setParameterActivenessChangeDetail: SetParameter[ActivenessChangeDetail] =
      (v, pp) => {
        pp >> v.name
        pp >> v.reassignmentCounterO
        pp >> v.remoteDomainIdxO
      }

    implicit val getResultChangeType: GetResult[ActivenessChangeDetail] = GetResult { r =>
      val operationName = r.nextString()
      val reassignmentCounterO = GetResult[Option[ReassignmentCounter]].apply(r)
      val remoteDomainO = r.nextIntOption()

      if (operationName == ActivenessChangeDetail.create.str) {
        val reassignmentCounter = reassignmentCounterO.getOrElse(
          throw new DbDeserializationException(
            "reassignment counter should be defined for a create"
          )
        )

        ActivenessChangeDetail.Create(reassignmentCounter)
      } else if (operationName == ActivenessChangeDetail.archive.str) {
        ActivenessChangeDetail.Archive
      } else if (operationName == ActivenessChangeDetail.add.str) {
        val reassignmentCounter = reassignmentCounterO.getOrElse(
          throw new DbDeserializationException("reassignment counter should be defined for an add")
        )

        ActivenessChangeDetail.Add(reassignmentCounter)
      } else if (operationName == ActivenessChangeDetail.purge.str) {
        ActivenessChangeDetail.Purge
      } else if (operationName == "assign" || operationName == "unassign") {
        val reassignmentCounter = reassignmentCounterO.getOrElse(
          throw new DbDeserializationException(
            s"reassignment counter should be defined for a $operationName"
          )
        )

        val remoteDomain = remoteDomainO.getOrElse(
          throw new DbDeserializationException(
            s"remote domain should be defined for a $operationName"
          )
        )

        if (operationName == ActivenessChangeDetail.assign.str)
          ActivenessChangeDetail.Assignment(reassignmentCounter, remoteDomain)
        else
          ActivenessChangeDetail.Unassignment(reassignmentCounter, remoteDomain)
      } else throw new DbDeserializationException(s"Unknown operation type [$operationName]")
    }
  }

  sealed trait AcsBaseError extends Product with Serializable

  /** Warning cases returned by the operations on the [[ActiveContractStore!]] */
  sealed trait AcsWarning extends AcsBaseError {
    // List of toc involved in the error
    def timeOfChanges: List[TimeOfChange]
  }

  /** Error cases returned by the operations on the [[ActiveContractStore!]] */
  sealed trait AcsError extends AcsBaseError

  final case class UnableToFindIndex(id: DomainId) extends AcsError

  final case class ActiveContractsDataInvariantViolation(
      errorMessage: String
  ) extends AcsError

  /** A contract is simultaneously created and/or reassigned from possibly several source domains */
  final case class SimultaneousActivation(
      contractId: LfContractId,
      toc: TimeOfChange,
      detail1: ActivenessChangeDetail,
      detail2: ActivenessChangeDetail,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(toc)
  }

  /** A contract is simultaneously archived and/or unassigned to possibly several source domains */
  final case class SimultaneousDeactivation(
      contractId: LfContractId,
      toc: TimeOfChange,
      detail1: ActivenessChangeDetail,
      detail2: ActivenessChangeDetail,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(toc)
  }

  /** The given contract is archived a second time, but with a different time of change. */
  final case class DoubleContractArchival(
      contractId: LfContractId,
      oldTime: TimeOfChange,
      newTime: TimeOfChange,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(oldTime, newTime)
  }

  /** The given contract is created a second time, but with a different time of change. */
  final case class DoubleContractCreation(
      contractId: LfContractId,
      oldTime: TimeOfChange,
      newTime: TimeOfChange,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(oldTime, newTime)

  }

  /** The state of a contract is changed before its `creation`. */
  final case class ChangeBeforeCreation(
      contractId: LfContractId,
      creation: TimeOfChange,
      change: TimeOfChange,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(creation, change)
  }

  /** The state of a contract is changed after its `archival`. */
  final case class ChangeAfterArchival(
      contractId: LfContractId,
      archival: TimeOfChange,
      change: TimeOfChange,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(archival, change)
  }

  /** ReassignmentCounter should increase monotonically with the time of change. */
  final case class ReassignmentCounterShouldIncrease(
      contractId: LfContractId,
      current: ReassignmentCounter,
      currentToc: TimeOfChange,
      next: ReassignmentCounter,
      nextToc: TimeOfChange,
      strict: Boolean,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(currentToc, nextToc)

    def reason: String =
      s"""The reassignment counter $current of the contract state at $currentToc should be smaller than ${if (
          strict
        ) ""
        else "or equal to "} the reassignment counter $next at $nextToc"""
  }

  /** Status of a contract in the ACS */
  sealed trait Status extends Product with Serializable with PrettyPrinting with HasPrunable {

    /** Returns whether pruning may delete a contract in this state */
    override def prunable: Boolean
    def isActive: Boolean = this match {
      case Active(_) => true
      case _ => false
    }

    def isReassignedAway: Boolean = this match {
      case ReassignedAway(_, _) => true
      case _ => false
    }
  }

  /** The contract has been created and is active. */
  final case class Active(reassignmentCounter: ReassignmentCounter) extends Status {
    override def prunable: Boolean = false

    override protected def pretty: Pretty[Active] = prettyOfClass(
      param("reassignment counter", _.reassignmentCounter)
    )
  }

  /** The contract has been archived and it is not active. */
  case object Archived extends Status {
    override def prunable: Boolean = true
    override protected def pretty: Pretty[Archived.type] = prettyOfObject[Archived.type]
    // reassignment counter remains None, because we do not write it back to the ACS
  }

  case object Purged extends Status {
    override def prunable: Boolean = true
    override protected def pretty: Pretty[Purged.type] = prettyOfObject[Purged.type]
  }

  /** The contract has been unassigned to the given `targetDomain` after it had resided on this domain.
    * It does not reside on the current domain, but the contract has existed at some time.
    *
    * In particular, this state does not imply any of the following:
    * <ul>
    *   <li>The reassignment was completed on the target domain.</li>
    *   <li>The contract now resides on the target domain.</li>
    *   <li>The contract is active or archived on any other domain.</li>
    * </ul>
    *
    * @param reassignmentCounter The reassignment counter of the unassignment request that reassigned the contract away.
    */
  final case class ReassignedAway(
      targetDomain: Target[DomainId],
      reassignmentCounter: ReassignmentCounter,
  ) extends Status {
    override def prunable: Boolean = true
    override protected def pretty: Pretty[ReassignedAway] = prettyOfClass(
      unnamedParam(_.targetDomain)
    )
  }

  sealed trait ReassignmentType extends Product with Serializable {
    def name: String
  }
  object ReassignmentType {
    case object Unassignment extends ReassignmentType {
      override def name: String = "unassignment"
    }
    case object Assignment extends ReassignmentType {
      override def name: String = "assignment"
    }
  }
  private[store] final case class ReassignmentCounterAtChangeInfo(
      timeOfChange: TimeOfChange,
      reassignmentCounter: Option[ReassignmentCounter],
  )

  private[store] def checkReassignmentCounterAgainstLatestBefore(
      contractId: LfContractId,
      timeOfChange: TimeOfChange,
      reassignmentCounter: ReassignmentCounter,
      latestBeforeO: Option[ReassignmentCounterAtChangeInfo],
  ): Checked[Nothing, ReassignmentCounterShouldIncrease, Unit] =
    latestBeforeO.flatMap { latestBefore =>
      latestBefore.reassignmentCounter.map { previousReassignmentCounter =>
        if (previousReassignmentCounter < reassignmentCounter) Checked.unit
        else {
          val error = ReassignmentCounterShouldIncrease(
            contractId,
            previousReassignmentCounter,
            latestBefore.timeOfChange,
            reassignmentCounter,
            timeOfChange,
            strict = true,
          )
          Checked.continue(error)
        }
      }
    }.sequence_

  private[store] def checkReassignmentCounterAgainstEarliestAfter(
      contractId: LfContractId,
      timeOfChange: TimeOfChange,
      reassignmentCounter: ReassignmentCounter,
      earliestAfterO: Option[ReassignmentCounterAtChangeInfo],
      reassignmentType: ReassignmentType,
  ): Checked[Nothing, ReassignmentCounterShouldIncrease, Unit] =
    earliestAfterO.flatMap { earliestAfter =>
      earliestAfter.reassignmentCounter.map { nextReassignmentCounter =>
        val (condition, strict) = reassignmentType match {
          case ReassignmentType.Assignment =>
            (reassignmentCounter <= nextReassignmentCounter) -> false
          case ReassignmentType.Unassignment =>
            (reassignmentCounter < nextReassignmentCounter) -> true
        }
        if (condition) Checked.unit
        else {
          val error = ReassignmentCounterShouldIncrease(
            contractId,
            reassignmentCounter,
            timeOfChange,
            nextReassignmentCounter,
            earliestAfter.timeOfChange,
            strict,
          )
          Checked.continue(error)
        }
      }
    }.sequence_

}

/** Provides snapshotting for active contracts. */
trait ActiveContractSnapshot {

  /** Returns all contracts that were active right after the given timestamp,
    * and when the contract became active for the last time before or at the given timestamp.
    *
    * @param timestamp The timestamp at which the snapshot shall be taken.
    *                  Must be before the timestamp that corresponds to the head cursor in the
    *                  [[com.digitalasset.canton.participant.protocol.RequestJournal]] for the state
    *                  [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]].
    *                  If this precondition is violated, the returned snapshot may be inconsistent, i.e.,
    *                  it may omit some contracts that were [[ActiveContractStore.Active]] at the given time
    *                  and it may include contracts that were actually [[ActiveContractStore.Archived]] or
    *                  [[ActiveContractStore.ReassignedAway]].
    * @return A map from contracts to the latest timestamp (no later than the given `timestamp`)
    *         when they became active again.
    *         It contains exactly those contracts that were active right after the given timestamp.
    *         If a contract is created or assigned and archived or unassigned at the same timestamp,
    *         it does not show up in any snapshot.
    *         The map is sorted by [[cats.kernel.Order]]`[`[[com.digitalasset.canton.protocol.LfContractId]]`]`.
    */
  def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (CantonTimestamp, ReassignmentCounter)]]

  /** Returns all contracts that were active right after the given request counter,
    * and when the contract became active for the last time before or at the given request counter.
    *
    * @param rc The request counter at which the snapshot shall be taken.
    *           Must be before the request counter that corresponds to the head cursor in the
    *           [[com.digitalasset.canton.participant.protocol.RequestJournal]] for the state
    *           [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]].
    *           If this precondition is violated, the returned snapshot may be inconsistent, i.e.,
    *           it may omit some contracts that were [[ActiveContractStore.Active]] at the given counter
    *           and it may include contracts that were actually [[ActiveContractStore.Archived]] or
    *           [[ActiveContractStore.ReassignedAway]].
    * @return A map from contracts to the latest request counter (no later than the given `rc`)
    *         when they became active again.
    *         It contains exactly those contracts that were active right after the given request counter.
    *         If a contract is created or assigned and archived or unassigned at the same request counter,
    *         it does not show up in any snapshot.
    *         The map is sorted by [[cats.kernel.Order]]`[`[[com.digitalasset.canton.protocol.LfContractId]]`]`.
    */
  def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, (RequestCounter, ReassignmentCounter)]]

  /** Returns the states of contracts at the given timestamp.
    *
    * @param contracts The contracts whose state we return. If empty, we return an empty map.
    *                  Omits from the response contracts that do not have an activeness state.
    * @return A map from contracts to the timestamp when the state changed, and the activeness change detail.
    *         The map is sorted by [[cats.kernel.Order]]`[`[[com.digitalasset.canton.protocol.LfContractId]]`]`.
    */
  def activenessOf(contracts: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]]

  /** Returns Some(contractId) if an active contract belonging to package `pkg` exists, otherwise returns None.
    * The returned contractId may be any active contract from package `pkg`.
    * The most recent contract state is used.
    */
  def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(LfContractId)]]

  /** Returns a map to the timestamp when the contract became active for the last time before or at the given timestamp.
    * Omits contracts that not active right after the given timestamp.
    *
    * @param timestamp The timestamp at which the activeness of the contracts shall be determined.
    *                  Must be before the timestamp that corresponds to the head cursor in the
    *                  [[com.digitalasset.canton.participant.protocol.RequestJournal]] for the state
    *                  [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]].
    *                  If this precondition is violated, the returned snapshot may be inconsistent, i.e.,
    *                  it may omit some contracts that were [[ActiveContractStore.Active]] at the given time
    *                  and it may include contracts that were actually [[ActiveContractStore.Archived]] or
    *                  [[ActiveContractStore.ReassignedAway]].
    */
  def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]]

  /** Returns a map to the latest reassignment counter of the contract before the given request counter.
    * Fails if not all given contract ids are active in the ACS, or if the ACS does not have defined their latest reassignment counter.
    *
    * @param requestCounter The request counter *immediately before* which the state of the contracts shall be determined.
    *
    * @throws java.lang.IllegalArgumentException if `requestCounter` is equal to RequestCounter.MinValue`,
    *         if not all given contract ids are active in the ACS,
    *         if the ACS does not contain the latest reassignment counter for each given contract id.
    */
  def bulkContractsReassignmentCounterSnapshot(
      contractIds: Set[LfContractId],
      requestCounter: RequestCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ReassignmentCounter]]

  /** Returns all changes to the active contract set between the two timestamps
    * (exclusive lower bound timestamp, inclusive upper bound timestamp)
    * in the order of their changes.
    * The provided lower bound must not be larger than the upper bound.
    *
    * @param fromExclusive The lower bound for the changes. Must not be larger than the upper bound.
    * @param toInclusive The upper bound for the changes. Must not be smaller than the lower bound.
    * @throws java.lang.IllegalArgumentException If the intervals are in the wrong order.
    */
  /*
   * In contrast to creates, assignments and unassignments, the DB does not store reassignment counters
   * for archivals. As such, to retrieve reassignment counters for archivals, for every contract `cid`
   * archived with request counter `rc`, we retrieve the biggest reassignment counter from an activation event
   * (create or assign) with a request counter rc' <= rc.
   * Some contracts archived between (`fromExclusive`, `toInclusive`] might have been activated last at time
   * lastActivationTime <= `fromExclusive`. Therefore, for retrieving the reassignment counter of archived contracts,
   * we may need to look at activations between (`fromExclusive`, `toInclusive`], and at activations taking
   * place at a time <= `fromExclusive`.
   *
   * The implementation assumes that:
   * - reassignment counters for a contract are strictly increasing for different activations of that contract
   * - the activations/deactivations retrieved from the DB really describe a well-formed
   * set of contracts, e.g., a contract is not archived twice, etc TODO(i12904)
   */
  def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]]

}

object ActiveContractSnapshot {

  final case class ActiveContractIdsChange(
      activations: Map[LfContractId, StateChangeType],
      deactivations: Map[LfContractId, StateChangeType],
  )

  object ActiveContractIdsChange {
    val empty = ActiveContractIdsChange(Map.empty, Map.empty)
  }

}

sealed trait ContractChange extends Product with Serializable with PrettyPrinting {
  override protected def pretty: Pretty[ContractChange.this.type] = prettyOfObject[this.type]
}
object ContractChange {
  case object Created extends ContractChange
  case object Archived extends ContractChange
  case object Purged extends ContractChange
  case object Unassigned extends ContractChange
  case object Assigned extends ContractChange
}

/** Type of state change of a contract as returned by [[com.digitalasset.canton.participant.store.ActiveContractStore.changesBetween]]
  * through a [[com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange]]
  */
final case class StateChangeType(change: ContractChange, reassignmentCounter: ReassignmentCounter)
    extends PrettyPrinting {
  override protected def pretty: Pretty[StateChangeType] =
    prettyOfClass(
      param("operation", _.change),
      param("reassignment counter", _.reassignmentCounter),
    )
}
