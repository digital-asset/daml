// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.inmemory

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.ledger.api.domain.{ApplicationId, CommandId}
import com.digitalasset.ledger.backend.api.v1.{RejectionReason, TransactionSubmission}
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveContracts
import com.digitalasset.platform.sandbox.stores.deduplicator.Deduplicator
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.{Checkpoint, Rejection}
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, LedgerEntry, LedgerSnapshot}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/** This stores all the mutable data that we need to run a ledger: the PCS, the ACS, and the deduplicator.
  *
  */
class InMemoryLedger(
    val ledgerId: String,
    timeProvider: TimeProvider,
    acs0: ActiveContracts,
    ledgerEntries: Seq[LedgerEntry])
    extends Ledger {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val entries = {
    val l = new LedgerEntries[LedgerEntry](_.toString)
    ledgerEntries.foreach(l.publish)
    l
  }

  override def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed] =
    entries.getSource(offset)

  // mutable state
  private var acs = acs0
  private var deduplicator = Deduplicator()

  override def ledgerEnd: Long = entries.ledgerEnd

  // need to take the lock to make sure the two pieces of data are consistent.
  override def snapshot(): Future[LedgerSnapshot] =
    Future.successful(this.synchronized {
      LedgerSnapshot(entries.ledgerEnd, Source(acs.contracts))
    })

  override def lookupContract(
      contractId: AbsoluteContractId): Future[Option[ActiveContracts.ActiveContract]] =
    Future.successful(this.synchronized {
      acs.contracts.get(contractId)
    })

  override def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]] =
    Future.successful(this.synchronized {
      acs.keys.get(key)
    })

  override def publishHeartbeat(time: Instant): Future[Unit] =
    Future.successful(this.synchronized[Unit] {
      entries.publish(Checkpoint(time))
      ()
    })

  override def publishTransaction(tx: TransactionSubmission): Future[Unit] =
    Future.successful(
      this.synchronized[Unit] {
        val (newDeduplicator, isDuplicate) =
          deduplicator.checkAndAdd(ApplicationId(tx.applicationId), CommandId(tx.commandId))
        deduplicator = newDeduplicator
        if (isDuplicate)
          logger.warn(
            "Ignoring duplicate submission for applicationId {}, commandId {}",
            tx.applicationId: Any,
            tx.commandId)
        else
          handleSuccessfulTx(entries.ledgerEnd.toString, tx)

      }
    )

  private def handleSuccessfulTx(transactionId: String, tx: TransactionSubmission): Unit = {

    val toAbsCoid: ContractId => AbsoluteContractId =
      SandboxEventIdFormatter.makeAbsCoid(transactionId)
    val mappedTx = tx.transaction.mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
    // 5b. modify the ActiveContracts, while checking that we do not have double
    // spends or timing issues
    val acsRes = acs.addTransaction(
      let = tx.ledgerEffectiveTime,
      workflowId = tx.workflowId,
      transactionId = transactionId,
      transaction = mappedTx,
      explicitDisclosure = tx.blindingInfo.explicitDisclosure
    )
    acsRes match {
      case Left(err) =>
        //TODO: this is sloppy here, errors can be TimeBeforeErrors as well
        handleError(
          tx,
          RejectionReason.Inconsistent(
            s"Contract dependencies inactive: ${err.mkString("[", ", ", "]")}"))
      case Right(newAcs) =>
        acs = newAcs
        val recordTx = mappedTx
          .mapNodeId(SandboxEventIdFormatter.fromTransactionId(transactionId, _))
        val recordBlinding =
          tx.blindingInfo.explicitDisclosure.map {
            case (nid, parties) =>
              (SandboxEventIdFormatter.fromTransactionId(transactionId, nid), parties)
          }
        val entry = LedgerEntry
          .Transaction(
            tx.commandId,
            transactionId,
            tx.applicationId,
            tx.submitter,
            tx.workflowId,
            tx.ledgerEffectiveTime,
            timeProvider.getCurrentTime,
            recordTx,
            recordBlinding.mapValues(_.map(_.underlyingString))
          )
        entries.publish(entry)
        ()
    }

  }

  private def handleError(tx: TransactionSubmission, reason: RejectionReason): Unit = {
    logger.warn(s"Publishing error to ledger: ${reason.description}")
    entries.publish(
      Rejection(timeProvider.getCurrentTime, tx.commandId, tx.applicationId, tx.submitter, reason)
    )
    ()
  }

}
