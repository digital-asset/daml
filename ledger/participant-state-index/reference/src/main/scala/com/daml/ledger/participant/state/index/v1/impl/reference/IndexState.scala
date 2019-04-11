// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import com.daml.ledger.participant.state.index.v1.Offset
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.daml.lf.transaction.{BlindingInfo, Transaction}
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.types.Ledger.LedgerFeatureFlags
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf.DamlLf.Archive
import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap

final case class IndexState(
    private val ledgerId: Option[LedgerId],
    private val updateId: Option[UpdateId],
    private val beginning: Option[UpdateId],
    private val configuration: Option[Configuration],
    private val recordTime: Option[Timestamp],
    // Accepted transactions indexed by offset.
    txs: TreeMap[Offset, (Update.TransactionAccepted, BlindingInfo)],
    activeContracts: Map[
      AbsoluteContractId,
      Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]],
    // Rejected commands indexed by offset.
    rejections: TreeMap[Offset, Update.CommandRejected],
    // Uploaded packages.
    packages: Map[PackageId, Archive],
    ledgerFeatureFlags: LedgerFeatureFlags,
    packageKnownTo: Relation[PackageId, Party],
    hostedParties: Set[Party]) {

  val logger = LoggerFactory.getLogger(this.getClass)

  import IndexState._

  /** Return True if the mandatory fields have been initialized. */
  def initialized: Boolean = {
    return this.ledgerId.isDefined && this.updateId.isDefined && this.recordTime.isDefined
    // FIXME(JM): && this.configuration.isDefined
  }

  private def getIfInitialized[T](x: Option[T]): T =
    x.getOrElse(sys.error("INTERNAL ERROR: State not yet initialized."))

  def getLedgerId: LedgerId = getIfInitialized(ledgerId)
  def getUpdateId: UpdateId = getIfInitialized(updateId)
  def getBeginning: UpdateId = getIfInitialized(beginning)
  def getConfiguration: Configuration = getIfInitialized(configuration)
  def getRecordTime: Timestamp = getIfInitialized(recordTime)

  /** Return a new state with the given update applied or the
    * invariant violation in case that is not possible.
    */
  def tryApply(uId: UpdateId, u0: Update): Either[InvariantViolation, IndexState] = {
    val state = this.copy(
      updateId = Some(uId),
      beginning = if (this.beginning.isEmpty) Some(uId) else this.beginning
    )
    // apply update to state with new offset
    u0 match {
      case u: Update.Heartbeat =>
        if (this.recordTime.fold(true)(_ <= u.recordTime))
          Right(state.copy(recordTime = Some(u.recordTime)))
        else
          Left(NonMonotonicRecordTimeUpdate)

      case u: Update.StateInit =>
        if (!this.ledgerId.isDefined)
          Right(state.copy(ledgerId = Some(u.ledgerId)))
        else
          Left(StateAlreadyInitialized)

      case u: Update.ConfigurationChanged =>
        Right(state.copy(configuration = Some(u.newConfiguration)))

      case u: Update.PartyAddedToParticipant =>
        Right(state.copy(hostedParties = state.hostedParties + u.party))

      case Update.PublicPackageUploaded(archive) =>
        val newPackages =
          state.packages + (Ref.PackageId.assertFromString(archive.getHash) -> archive)

        val decodedPackages = newPackages.mapValues(archive => Decode.decodeArchive(archive)._2)
        val newLedgerFeatureFlags = LedgerFeatureFlags
          .fromPackages(decodedPackages)
          .getOrElse(sys.error("FIXME(JM): Mixed ledger feature flags"))

        logger.debug(s"ledger feature flags = $newLedgerFeatureFlags")

        Right(
          state
            .copy(
              packages = newPackages,
              ledgerFeatureFlags = newLedgerFeatureFlags
            ))

      case u: Update.CommandRejected =>
        Right(
          state.copy(
            rejections = rejections + (Offset.fromUpdateId(uId) -> u)
          )
        )
      case u: Update.TransactionAccepted =>
        val blindingInfo = Blinding.blind(
          state.ledgerFeatureFlags,
          u.transaction.asInstanceOf[Transaction.Transaction]
        )
        logger.debug(s"blindingInfo=$blindingInfo")
        Right(
          state.copy(
            txs = txs + (Offset.fromUpdateId(uId) -> ((u, blindingInfo))),
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
    ledgerId = None,
    updateId = None,
    beginning = None,
    configuration = None,
    recordTime = None,
    txs = TreeMap.empty,
    activeContracts = Map.empty,
    rejections = TreeMap.empty,
    packages = Map.empty,
    ledgerFeatureFlags = LedgerFeatureFlags.default,
    packageKnownTo = Map.empty,
    hostedParties = Set.empty
  )
}
