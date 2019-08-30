// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import com.daml.ledger.participant.state.index.v1.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.PartyDetails
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.TreeMap

final case class IndexState(
    ledgerId: LedgerId,
    participantId: ParticipantId,
    recordTime: Timestamp,
    configuration: Configuration,
    private val updateId: Option[Offset],
    private val beginning: Option[Offset],
    // Accepted transactions indexed by offset.
    txs: TreeMap[Offset, (Update.TransactionAccepted, BlindingInfo)],
    activeContracts: InMemoryActiveLedgerState,
    // Rejected commands indexed by offset.
    commandRejections: TreeMap[Offset, Update.CommandRejected],
    // Uploaded packages.
    packages: Map[PackageId, Archive],
    packageDetails: Map[PackageId, PackageDetails],
    knownParties: Set[PartyDetails]) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import IndexState._

  /** Return True if the mandatory fields have been initialized. */
  def initialized: Boolean =
    this.updateId.isDefined && this.beginning.isDefined

  private def getIfInitialized[T](x: Option[T]): T =
    x.getOrElse(sys.error("INTERNAL ERROR: State not yet initialized."))

  def getUpdateId: Offset = getIfInitialized(updateId)
  def getBeginning: Offset = getIfInitialized(beginning)

  /** Return a new state with the given update applied or the
    * invariant violation in case that is not possible.
    */
  def tryApply(uId: Offset, u0: Update): Either[InvariantViolation, IndexState] = {
    if (this.updateId.fold(false)(uId <= _))
      Left(NonMonotonicOffset)
    else {
      val state = this.copy(
        updateId = Some(uId),
        beginning = if (this.beginning.isEmpty) Some(uId) else this.beginning
      )
      // apply update to state with new offset
      u0 match {
        case u: Update.Heartbeat =>
          if (this.recordTime <= u.recordTime)
            Right(state.copy(recordTime = u.recordTime))
          else
            Left(NonMonotonicRecordTimeUpdate)

        case u: Update.ConfigurationChanged =>
          Right(state.copy(configuration = u.newConfiguration))

        case _: Update.ConfigurationChangeRejected =>
          Right(state)

        case u: Update.PartyAddedToParticipant =>
          Right(
            state.copy(
              knownParties = state.knownParties + PartyDetails(
                u.party,
                Some(u.displayName),
                u.participantId == state.participantId)))

        case Update.PublicPackageUploaded(archive, sourceDescription, _, uploadRecordTime) =>
          val newPackages =
            state.packages + (PackageId.assertFromString(archive.getHash) -> archive)

          val newPackageDetails =
            state.packageDetails +
              (PackageId.assertFromString(archive.getHash) -> PackageDetails(
                archive.getPayload.size.toLong,
                uploadRecordTime,
                sourceDescription))

          Right(
            state
              .copy(
                packages = newPackages,
                packageDetails = newPackageDetails
              ))

        case u: Update.CommandRejected =>
          Right(
            state.copy(
              commandRejections = commandRejections + (uId -> u)
            )
          )

        case u: Update.TransactionAccepted =>
          val blindingInfo = Blinding.blind(
            u.transaction.mapContractId(cid => cid: ContractId)
          )
          activeContracts
            .addTransaction(
              let = u.transactionMeta.ledgerEffectiveTime.toInstant,
              transactionId = u.transactionId,
              workflowId = u.transactionMeta.workflowId,
              transaction = u.transaction,
              explicitDisclosure = blindingInfo.explicitDisclosure,
              localImplicitDisclosure = blindingInfo.localImplicitDisclosure,
              globalImplicitDisclosure = blindingInfo.globalImplicitDisclosure
            )
            .fold(_ => Left(SequencingError), { newActiveContracts =>
              Right(
                state.copy(
                  txs = txs + (uId -> ((u, blindingInfo))),
                  activeContracts = newActiveContracts
                ))
            })
      }
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
  case object NonMonotonicOffset extends InvariantViolation
  case object SequencingError extends InvariantViolation

  def initialState(lic: LedgerInitialConditions, givenParticipantId: ParticipantId): IndexState =
    IndexState(
      ledgerId = lic.ledgerId,
      participantId = givenParticipantId,
      updateId = None,
      beginning = None,
      configuration = lic.config,
      recordTime = lic.initialRecordTime,
      txs = TreeMap.empty,
      activeContracts = InMemoryActiveLedgerState.empty,
      commandRejections = TreeMap.empty,
      packages = Map.empty,
      packageDetails = Map.empty,
      knownParties = Set.empty
    )

}
