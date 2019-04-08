// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import java.util.UUID

import com.daml.ledger.participant.state.index.v1.{IndexId, Offset}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.{BlindingInfo, Transaction}
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf.DamlLf.Archive

import scala.collection.immutable.TreeMap

final case class IndexState(
    // TODO(SM):
    // - use immutable collections with the right asymptotics
    // - understand how to handle rejection
    indexId: IndexId,
    private val stateId: Option[StateId],
    private val updateId: Option[UpdateId],
    // The next ledger offset.
    nextOffset: Offset,
    // Mapping from the ledger offset to the opaque update id.
    offsetToUpdateId: Map[Long, UpdateId],
    private val configuration: Option[Configuration],
    recordTime: Option[Timestamp],
    // Accepted transactions indexed by UpdateId.
    txs: TreeMap[Offset, (Update.TransactionAccepted, BlindingInfo)],
    activeContracts: Map[
      AbsoluteContractId,
      Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]],
    // Rejected commands indexed by UpdateId.
    rejections: TreeMap[Offset, Update.CommandRejected],
    // Uploaded packages.
    packages: Map[PackageId, (Option[SubmitterInfo], Archive)],
    packageKnownTo: Relation[PackageId, Party],
    hostedParties: Set[Party]) {

  import IndexState._

  /** Return True if the mandatory fields have been initialized. */
  def initialized: Boolean = {
    return this.stateId.isDefined && this.updateId.isDefined && this.recordTime.isDefined
    // FIXME(JM): && this.configuration.isDefined
  }

  def getStateId: StateId =
    stateId.getOrElse(sys.error("INTERNAL ERROR: State not yet initialized."))

  def getUpdateId: UpdateId =
    updateId.getOrElse(sys.error("INTERNAL ERROR: State not yet initialized."))

  def getConfiguration: Configuration =
    configuration.getOrElse(sys.error("INTERNAL ERROR: State not yet initialized."))

  /** Return a new state with the given update applied or the
    * invariant violation in case that is not possible.
    */
  def tryApply(uId: UpdateId, u0: Update): Either[InvariantViolation, IndexState] = {
    // record new offset
    val offset = this.nextOffset
    val state = this.copy(
      updateId = Some(uId),
      nextOffset = this.nextOffset + 1,
      offsetToUpdateId = this.offsetToUpdateId + (offset -> uId)
    )
    // apply update to state with new offset
    u0 match {
      case u: Update.Heartbeat =>
        if (this.recordTime.fold(true)(_ <= u.recordTime))
          Right(state.copy(recordTime = Some(u.recordTime)))
        else
          Left(NonMonotonicRecordTimeUpdate)

      case u: Update.StateInit =>
        if (!this.stateId.isDefined)
          Right(state.copy(stateId = Some(u.stateId)))
        else
          Left(StateAlreadyInitialized)

      case u: Update.ConfigurationChanged =>
        Right(state.copy(configuration = Some(u.newConfiguration)))

      case u: Update.PartyAddedToParticipant =>
        Right(state.copy(hostedParties = state.hostedParties + u.party))

      case u: Update.PackageUploaded =>
        Right(state.copy(packages = state.packages +
          (Ref.PackageId.assertFromString(u.archive.getHash) -> ((u.optSubmitterInfo, u.archive)))))

      case u: Update.CommandRejected =>
        Right(
          state.copy(
            rejections = rejections + (offset -> u)
          )
        )
      case u: Update.TransactionAccepted =>
        val blindingInfo = Blinding.blind(
          Ledger.LedgerFeatureFlags.default, /* FIXME(JM) */
          u.transaction.asInstanceOf[Transaction.Transaction]
        )
        Right(
          state.copy(
            txs = txs + (offset -> ((u, blindingInfo))),
            activeContracts =
              activeContracts -- consumedContracts(u.transaction) ++ createdContracts(u.transaction)
          )
        )
    }
  }

  private def consumedContracts(tx: CommittedTransaction): Iterable[Value.AbsoluteContractId] =
    tx.nodes.values
      .collect {
        case e: NodeExercises[
              NodeId,
              Value.AbsoluteContractId,
              Value.VersionedValue[Value.AbsoluteContractId]] if e.consuming =>
          e.targetCoid
      }

  private def createdContracts(tx: CommittedTransaction): Iterable[(
      Value.AbsoluteContractId,
      Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]])] =
    tx.nodes.values
      .collect {
        case c: NodeCreate[
              Value.AbsoluteContractId,
              Value.VersionedValue[Value.AbsoluteContractId]] =>
          (c.coid, c.coinst)
      }
}

object IndexState {
  sealed trait InvariantViolation
  case object NonMonotonicRecordTimeUpdate extends InvariantViolation
  case object StateAlreadyInitialized extends InvariantViolation

  def initialState: IndexState = IndexState(
    indexId = UUID.randomUUID().toString,
    stateId = None,
    updateId = None,
    nextOffset = 0,
    offsetToUpdateId = Map.empty,
    configuration = None,
    recordTime = None,
    txs = TreeMap.empty,
    activeContracts = Map.empty,
    rejections = TreeMap.empty,
    packages = Map.empty,
    packageKnownTo = Map.empty,
    hostedParties = Set.empty
  )
}
