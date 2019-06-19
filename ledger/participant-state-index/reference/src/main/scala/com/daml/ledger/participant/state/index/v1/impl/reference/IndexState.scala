// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import com.daml.ledger.participant.state.v1.{Offset, _}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.transaction.{BlindingInfo, Transaction}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.sandbox.stores.InMemoryActiveContracts
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.TreeMap

final case class IndexState(
    ledgerId: LedgerId,
    recordTime: Timestamp,
    configuration: Configuration,
    private val updateId: Option[Offset],
    private val beginning: Option[Offset],
    // Accepted transactions indexed by offset.
    txs: TreeMap[Offset, (Update.TransactionAccepted, BlindingInfo)],
    activeContracts: InMemoryActiveContracts,
    // Rejected commands indexed by offset.
    rejections: TreeMap[Offset, Update.CommandRejected],
    // Uploaded packages.
    packages: Map[PackageId, Archive],
    packageKnownTo: Relation[PackageId, Party],
    hostedParties: Set[Party]) {

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

        case u: Update.PartyAddedToParticipant =>
          Right(state.copy(hostedParties = state.hostedParties + u.party))

        case Update.PublicPackagesUploaded(
            archives,
            sourceDescription,
            participantId,
            recordTime) =>
          val newPackages =
            state.packages ++ archives.map(a => PackageId.assertFromString(a.getHash) -> a)

          val decodedPackages = newPackages.mapValues(archive => Decode.decodeArchive(archive)._2)
          Right(
            state
              .copy(
                packages = newPackages
              ))

        case u: Update.CommandRejected =>
          Right(
            state.copy(
              rejections = rejections + (uId -> u)
            )
          )
        case u: Update.TransactionAccepted =>
          val blindingInfo = Blinding.blind(
            // FIXME(JM): Make Blinding.blind polymorphic.
            u.transaction.asInstanceOf[Transaction.Transaction]
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

  def initialState(lic: LedgerInitialConditions): IndexState = IndexState(
    ledgerId = lic.ledgerId,
    updateId = None,
    beginning = None,
    configuration = lic.config,
    recordTime = lic.initialRecordTime,
    txs = TreeMap.empty,
    activeContracts = InMemoryActiveContracts.empty,
    rejections = TreeMap.empty,
    packages = Map.empty,
    packageKnownTo = Map.empty,
    hostedParties = Set.empty
  )

}
