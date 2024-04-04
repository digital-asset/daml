// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String100, String36}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail.ChangeType
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.{LfContractId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.CheckedT
import com.digitalasset.canton.util.FutureInstances.*
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** <p>The active contract store (ACS) stores for every contract ID
  * whether it is inexistent, [[ActiveContractStore.Active]],
  * [[ActiveContractStore.Archived]], or [[ActiveContractStore.TransferredAway]],
  * along with the timestamp of the latest change.
  * Every change is associated with the timestamp and request counter of the request that triggered the change.
  * The changes are ordered first by timestamp, then by request counter, and finally by change type (activations before deactivations).
  * Implementations must be thread-safe.
  * Updates must be idempotent.</p>
  *
  * <p>Creations, transfers, and archivals can be mixed arbitrarily.
  * A contract may be transferred-in and -out several times during its lifetime.
  * It becomes active with every transfer-in and transferred away with every transfer-out.
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
    *           <li>[[ActiveContractStore.SimultaneousActivation]] if the contract is transferred in at the same time
    *             or has been created by a different request at the same time.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] for every change that occurs before the creation timestamp.
    *             This is reported only if no [[ActiveContractStore.DoubleContractCreation]] is reported.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] if this creation is later than the earliest archival of the contract.</li>
    *         </ul>
    */
  def markContractsCreated(contracts: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsCreatedOrAdded(contracts, toc: TimeOfChange, isCreation = true)

  /** Shorthand for `markContractsCreated(Seq(contract), toc)` */
  def markContractCreated(contract: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsCreatedOrAdded(Seq(contract), toc, isCreation = true)

  /** Shorthand for `markContractAdded(Seq(contract), toc)` */
  def markContractAdded(contract: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsAdded(Seq(contract), toc)

  /** Marks the given contracts as active from `timestamp` (inclusive) onwards.
    *
    * Unlike creation, add can be done several times in the life of a contract.
    * It is intended to use from the repair service.
    */
  def markContractsAdded(contracts: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    markContractsCreatedOrAdded(contracts, toc: TimeOfChange, isCreation = false)

  protected def markContractsCreatedOrAdded(
      contracts: Seq[LfContractId],
      toc: TimeOfChange,
      isCreation: Boolean, // true if create, false if add
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit]

  /** Marks the given contracts as archived from `toc`'s timestamp (inclusive) onwards.
    *
    * @param contractIds The contract IDs of the contracts to be archived
    * @param toc The time of change consisting of
    *            <ul>
    *              <li>The request counter of the confirmation request that archives the contracts.</li>
    *              <li>The timestamp on the confirmation request that archives the contracts.</li>
    *            </ul>
    * @return The future completes when all contract states have been updated.
    *         The following irregularities are reported for each contract:
    *         <ul>
    *           <li>[[ActiveContractStore.DoubleContractArchival]] if the contract is archived a second time.</li>
    *           <li>[[ActiveContractStore.SimultaneousDeactivation]] if the contract is transferred out at the same time
    *             or has been archived by a different request at the same time.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] for every change that occurs after the archival timestamp.
    *             This is reported only if no [[ActiveContractStore.DoubleContractArchival]] is reported.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] if this archival is earlier than the latest creation of the contract.</li>
    *         </ul>
    */
  def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    purgeOrArchiveContracts(contractIds, toc, isArchival = true)

  /** Shorthand for `archiveContracts(Seq(cid), toc)` */
  def archiveContract(cid: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    archiveContracts(Seq(cid), toc)

  /** Shorthand for `purgeContracts(Seq(cid), toc)` */
  def purgeContract(cid: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    purgeOrArchiveContracts(Seq(cid), toc, isArchival = false)

  /** Marks the given contracts as inactive from `timestamp` (inclusive) onwards.
    *
    * Unlike archival, purge can be done several times in the life of a contract.
    * It is intended to use from the repair service.
    */
  def purgeContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    purgeOrArchiveContracts(contractIds, toc, isArchival = false)

  /** Depending on the `isArchival`, will archive (effect of a Daml transaction) or purge (repair service)
    */
  protected def purgeOrArchiveContracts(
      contractIds: Seq[LfContractId],
      toc: TimeOfChange,
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
    * If a contract is created or transferred-in and archived or transferred-out at the same [[com.digitalasset.canton.participant.util.TimeOfChange]],
    * the contract is [[ActiveContractStore.Archived]] or [[ActiveContractStore.TransferredAway]].
    * A contract cannot be archived and transferred out at the same timestamp.
    *
    * @return The map from contracts in `contractIds` in the store to their latest state.
    *         Nonexistent contracts are excluded from the map.
    * @see ActiveContractSnapshot!.snapshot
    */
  def fetchStates(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ContractState]]

  /** Marks the given contracts as transferred in from `toc`'s timestamp (inclusive) onwards.
    *
    * @param transferIns The contract IDs to transfer-in, each with its source domain and time of change.
    * @return The future completes when the contract states have been updated.
    *         The following irregularities are reported:
    *         <ul>
    *           <li>[[ActiveContractStore.SimultaneousActivation]] if a transfer-in from another domain or a creation
    *             has been added with the same timestamp.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] if this timestamp is after the earliest archival of the contract.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] if this timestamp is before the latest creation of the contract.</li>
    *         </ul>
    */
  def transferInContracts(
      transferIns: Seq[(LfContractId, SourceDomainId, TimeOfChange)]
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit]

  def transferInContract(
      contractId: LfContractId,
      toc: TimeOfChange,
      sourceDomain: SourceDomainId,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    transferInContracts(Seq((contractId, sourceDomain, toc)))

  /** Marks the given contracts as [[ActiveContractStore.TransferredAway]] from `toc`'s timestamp (inclusive) onwards.
    *
    * @param transferOuts The contract IDs to transfer out, each with its target domain and time of change.
    * @return The future completes when the contract state has been updated.
    *         The following irregularities are reported:
    *         <ul>
    *           <li>[[ActiveContractStore.SimultaneousDeactivation]] if a transfer-out to another domain or a creation
    *             has been added with the same timestamp.</li>
    *           <li>[[ActiveContractStore.ChangeAfterArchival]] if this timestamp is after the earliest archival of the contract.</li>
    *           <li>[[ActiveContractStore.ChangeBeforeCreation]] if this timestamp is before the latest creation of the contract.</li>
    *         </ul>
    */
  def transferOutContracts(
      transferOuts: Seq[(LfContractId, TargetDomainId, TimeOfChange)]
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit]

  def transferOutContract(
      contractId: LfContractId,
      toc: TimeOfChange,
      targetDomain: TargetDomainId,
  )(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    transferOutContracts(Seq((contractId, targetDomain, toc)))

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
    * This method must not be called concurrently with creating, archiving, or transferring contracts.
    *
    * Therefore, this method need not be linearizable w.r.t. creating, archiving, or transferring contracts.
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

  protected def domainIdFromIdx(
      idx: Int
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): Future[DomainId] =
    IndexedDomain
      .fromDbIndexOT("active_contracts remote domain index", indexedStringStore)(idx)
      .map(_.domainId)
      .value
      .flatMap {
        case Some(domainId) => Future.successful(domainId)
        case None =>
          Future.failed(
            new RuntimeException(s"Unable to find domain ID for domain with index $idx")
          )
      }

  protected def getDomainIndices(
      domains: Seq[DomainId]
  ): CheckedT[Future, AcsError, AcsWarning, Map[DomainId, IndexedDomain]] =
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

  sealed trait ActivenessChangeDetail extends Product with Serializable {
    def name: LengthLimitedString

    def remoteDomainIdxO: Option[Int]

    def changeType: ChangeType
    def contractChange: ContractChange

    def isTransfer: Boolean
  }

  object ActivenessChangeDetail {
    val create: String36 = String36.tryCreate("create")
    val archive: String36 = String36.tryCreate("archive")
    val add: String36 = String36.tryCreate("add")
    val purge: String36 = String36.tryCreate("purge")
    val transferIn: String36 = String36.tryCreate("transfer-in")
    val transferOut: String36 = String36.tryCreate("transfer-out")

    sealed trait ChangeType {
      def name: String

      // lazy val so that `kind` is initialized first in the subclasses
      final lazy val toDbPrimitive: String100 =
        // The Oracle DB schema allows up to 100 chars; Postgres, H2 map this to an enum
        String100.tryCreate(name)
    }

    object ChangeType {
      case object Activation extends ChangeType {
        override val name = "activation"
      }

      case object Deactivation extends ChangeType {
        override val name = "deactivation"
      }

      implicit val setParameterChangeType: SetParameter[ChangeType] = (v, pp) =>
        pp >> v.toDbPrimitive
      implicit val getResultChangeType: GetResult[ChangeType] = GetResult(r =>
        r.nextString() match {
          case ChangeType.Activation.name => ChangeType.Activation
          case ChangeType.Deactivation.name => ChangeType.Deactivation
          case unknown => throw new DbDeserializationException(s"Unknown change type [$unknown]")
        }
      )
    }

    sealed trait TransferChangeDetail extends ActivenessChangeDetail {
      def toTransferType: ActiveContractStore.TransferType
      def remoteDomainIdx: Int
      override def remoteDomainIdxO: Option[Int] = Some(remoteDomainIdx)

      override def isTransfer: Boolean = true
    }

    case object Create extends ActivenessChangeDetail {
      override val name = ActivenessChangeDetail.create
      override def remoteDomainIdxO: Option[Int] = None

      override def changeType: ChangeType = ChangeType.Activation

      override def contractChange: ContractChange = ContractChange.Created

      override def isTransfer: Boolean = false
    }

    case object Add extends ActivenessChangeDetail {
      override val name = ActivenessChangeDetail.add
      override def remoteDomainIdxO: Option[Int] = None

      override def changeType: ChangeType = ChangeType.Activation

      override def contractChange: ContractChange = ContractChange.Created

      override def isTransfer: Boolean = false
    }

    case object Archive extends ActivenessChangeDetail {
      override val name = ActivenessChangeDetail.archive
      override def remoteDomainIdxO: Option[Int] = None
      override def changeType: ChangeType = ChangeType.Deactivation

      override def contractChange: ContractChange = ContractChange.Archived

      override def isTransfer: Boolean = false
    }

    case object Purge extends ActivenessChangeDetail {
      override val name = ActivenessChangeDetail.purge
      override def remoteDomainIdxO: Option[Int] = None

      override def changeType: ChangeType = ChangeType.Deactivation

      override def contractChange: ContractChange = ContractChange.Created

      override def isTransfer: Boolean = false
    }

    final case class TransferIn(remoteDomainIdx: Int) extends TransferChangeDetail {
      override val name = ActivenessChangeDetail.transferIn

      override def changeType: ChangeType = ChangeType.Activation
      override def toTransferType: ActiveContractStore.TransferType =
        ActiveContractStore.TransferType.TransferIn

      override def contractChange: ContractChange = ContractChange.TransferOut
    }

    final case class TransferOut(remoteDomainIdx: Int) extends TransferChangeDetail {
      override val name = ActivenessChangeDetail.transferOut

      override def changeType: ChangeType = ChangeType.Deactivation
      override def toTransferType: ActiveContractStore.TransferType =
        ActiveContractStore.TransferType.TransferOut

      override def contractChange: ContractChange = ContractChange.TransferIn
    }

    implicit val setParameterActivenessChangeDetail: SetParameter[ActivenessChangeDetail] =
      (v, pp) => {
        pp >> v.name
        pp >> v.remoteDomainIdxO
      }

    implicit val getResultChangeType: GetResult[ActivenessChangeDetail] = GetResult { r =>
      val operationName = r.nextString()
      val remoteDomainO = r.nextIntOption()

      if (operationName == ActivenessChangeDetail.create.str) {
        ActivenessChangeDetail.Create
      } else if (operationName == ActivenessChangeDetail.archive.str) {
        ActivenessChangeDetail.Archive
      } else if (operationName == ActivenessChangeDetail.add.str) {
        ActivenessChangeDetail.Add
      } else if (operationName == ActivenessChangeDetail.purge.str) {
        ActivenessChangeDetail.Purge
      } else if (operationName == "transfer-in" || operationName == "transfer-out") {
        val remoteDomain = remoteDomainO.getOrElse(
          throw new DbDeserializationException(
            s"remote domain should be defined for a $operationName"
          )
        )

        if (operationName == ActivenessChangeDetail.transferIn.str)
          ActivenessChangeDetail.TransferIn(remoteDomain)
        else
          ActivenessChangeDetail.TransferOut(remoteDomain)
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
  trait AcsError extends AcsBaseError

  final case class UnableToFindIndex(id: DomainId) extends AcsError

  final case class ActiveContractsDataInvariantViolation(
      errorMessage: String
  ) extends AcsError

  /** A contract is simultaneously created and/or transferred from possibly several source domains */
  final case class SimultaneousActivation(
      contractId: LfContractId,
      toc: TimeOfChange,
      detail1: ActivenessChangeDetail,
      detail2: ActivenessChangeDetail,
  ) extends AcsWarning {
    override def timeOfChanges: List[TimeOfChange] = List(toc)
  }

  /** A contract is simultaneously archived and/or transferred out to possibly several source domains */
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

  /** Status of a contract in the ACS */
  sealed trait Status extends Product with Serializable with PrettyPrinting with HasPrunable {

    /** Returns whether pruning may delete a contract in this state */
    override def prunable: Boolean
    def isActive: Boolean = this match {
      case Active => true
      case _ => false
    }
  }

  /** The contract has been created and is active. */
  case object Active extends Status {
    override def prunable: Boolean = false

    override def pretty: Pretty[Active.type] = prettyOfObject[Active.type]
  }

  /** The contract has been archived and it is not active. */
  case object Archived extends Status {
    override def prunable: Boolean = true
    override def pretty: Pretty[Archived.type] = prettyOfObject[Archived.type]
  }

  case object Purged extends Status {
    override def prunable: Boolean = true
    override def pretty: Pretty[Purged.type] = prettyOfObject[Purged.type]
  }

  /** The contract has been transferred out to the given `targetDomain` after it had resided on this domain.
    * It does not reside on the current domain, but the contract has existed at some time.
    *
    * In particular, this state does not imply any of the following:
    * <ul>
    *   <li>The transfer was completed on the target domain.</li>
    *   <li>The contract now resides on the target domain.</li>
    *   <li>The contract is active or archived on any other domain.</li>
    * </ul>
    */
  final case class TransferredAway(
      targetDomain: TargetDomainId
  ) extends Status {
    override def prunable: Boolean = true
    override def pretty: Pretty[TransferredAway] = prettyOfClass(unnamedParam(_.targetDomain))
  }

  private[store] sealed trait TransferType extends Product with Serializable {
    def name: String
  }
  private[store] object TransferType {
    case object TransferOut extends TransferType {
      override def name: String = "transfer-out"
    }
    case object TransferIn extends TransferType {
      override def name: String = "transfer-in"
    }
  }
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
    *                  [[ActiveContractStore.TransferredAway]].
    * @return A map from contracts to the latest timestamp (no later than the given `timestamp`)
    *         when they became active again.
    *         It contains exactly those contracts that were active right after the given timestamp.
    *         If a contract is created or transferred-in and archived or transferred-out at the same timestamp,
    *         it does not show up in any snapshot.
    *         The map is sorted by [[cats.kernel.Order]]`[`[[com.digitalasset.canton.protocol.LfContractId]]`]`.
    */
  def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, CantonTimestamp]]

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
    *           [[ActiveContractStore.TransferredAway]].
    * @return A map from contracts to the latest request counter (no later than the given `rc`)
    *         when they became active again.
    *         It contains exactly those contracts that were active right after the given request counter.
    *         If a contract is created or transferred-in and archived or transferred-out at the same request counter,
    *         it does not show up in any snapshot.
    *         The map is sorted by [[cats.kernel.Order]]`[`[[com.digitalasset.canton.protocol.LfContractId]]`]`.
    */
  def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, RequestCounter]]

  /** Returns Some(contractId) if an active contract belonging to package `pkg` exists, otherwise returns None.
    * The returned contractId may be any active contract from package `pkg`.
    * The most recent contract state is used.
    */
  def packageUsage(pkg: PackageId, contractStore: ContractStore)(implicit
      traceContext: TraceContext
  ): Future[Option[(LfContractId)]]

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
    *                  [[ActiveContractStore.TransferredAway]].
    */
  def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]]

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
  override def pretty: Pretty[ContractChange.this.type] = prettyOfObject[this.type]
}
object ContractChange {
  case object Created extends ContractChange
  case object Archived extends ContractChange
  case object Purged extends ContractChange
  case object TransferOut extends ContractChange
  case object TransferIn extends ContractChange
}

/** Type of state change of a contract as returned by [[com.digitalasset.canton.participant.store.ActiveContractStore.changesBetween]]
  * through a [[com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange]]
  */
final case class StateChangeType(change: ContractChange) extends PrettyPrinting {
  override def pretty: Pretty[StateChangeType] =
    prettyOfClass(param("", _.change))
}
