// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.{
  AcceptedTransaction,
  Heartbeat,
  RejectedCommand
}
import com.digitalasset.ledger.backend.api.v1._
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.stores.ActiveContracts

import scala.concurrent.Future

class SandboxLedgerBackend(ledger: Ledger)(implicit mat: Materializer) extends LedgerBackend {
  override def ledgerId: String = ledger.ledgerId

  private class SandboxSubmissionHandle extends SubmissionHandle {
    override def abort: Future[Unit] = Future.successful(())

    override def submit(submitted: TransactionSubmission): Future[Unit] = {
      ledger.publishTransaction(submitted)
    }

    override def lookupActiveContract(contractId: Value.AbsoluteContractId)
      : Future[Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]] =
      ledger.lookupContract(contractId).map(_.map(_.contract))(DirectExecutionContext)

    override def lookupContractKey(key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] =
      ledger.lookupKey(key)
  }

  override def beginSubmission(): Future[SubmissionHandle] =
    Future.successful(new SandboxSubmissionHandle)

  override def ledgerSyncEvents(
      offset: Option[LedgerSyncOffset]): Source[LedgerSyncEvent, NotUsed] =
    ledger
      .ledgerEntries(offset.map(_.toLong))
      .map { case (o, item) => toLedgerSyncEvent(o, item) }

  override def activeContractSetSnapshot()
    : Future[(LedgerSyncOffset, Source[ActiveContract, NotUsed])] =
    ledger
      .snapshot()
      .map {
        case LedgerSnapshot(offset, acsStream) =>
          (offset.toString, acsStream.map { case (cid, ac) => toSyncActiveContract(cid, ac) })
      }(mat.executionContext)

  override def getCurrentLedgerEnd: Future[LedgerSyncOffset] =
    Future.successful(ledger.ledgerEnd.toString)

  private def toSyncActiveContract(
      id: Value.AbsoluteContractId,
      ac: ActiveContracts.ActiveContract): ActiveContract =
    ActiveContract(
      id,
      ac.let,
      ac.transactionId,
      ac.workflowId,
      ac.contract,
      ac.witnesses
    )

  private def toLedgerSyncEvent(offset: Long, item: LedgerEntry): LedgerSyncEvent = {
    item match {
      case LedgerEntry.Rejection(
          recordTime,
          commandId,
          applicationId,
          submitter,
          rejectionReason) =>
        RejectedCommand(
          recordTime,
          commandId,
          submitter,
          rejectionReason,
          offset.toString,
          Some(applicationId))
      case LedgerEntry.Transaction(
          commandId,
          transactionId,
          applicationId,
          submittingParty,
          workflowId,
          ledgerEffectiveTime,
          recordedAt,
          transaction,
          explicitDisclosure) =>
        AcceptedTransaction(
          transaction,
          transactionId,
          Some(submittingParty),
          ledgerEffectiveTime,
          recordedAt,
          offset.toString,
          workflowId,
          explicitDisclosure.mapValues(_.map(Ref.Party.assertFromString)),
          Some(applicationId),
          Some(commandId)
        )
      case LedgerEntry.Checkpoint(recordedAt) =>
        Heartbeat(
          recordedAt,
          offset.toString
        )
    }
  }

  override def close(): Unit = {
    ledger.close()
  }
}
