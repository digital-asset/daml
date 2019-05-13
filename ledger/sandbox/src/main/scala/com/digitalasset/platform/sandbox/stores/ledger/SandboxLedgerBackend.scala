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
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.stores.ActiveContracts

import scala.concurrent.{ExecutionContext, Future}

class SandboxLedgerBackend(ledger: Ledger)(implicit mat: Materializer) extends LedgerBackend {
  override def ledgerId: String = ledger.ledgerId

  private class SandboxSubmissionHandle extends SubmissionHandle {
    override def abort: Future[Unit] = Future.successful(())

    override def submit(submitted: TransactionSubmission): Future[SubmissionResult] =
      ledger.publishTransaction(submitted)

    private[this] def canSeeContract(
        submitter: Party,
        ac: ActiveContracts.ActiveContract): Boolean = {
      // ^ only parties disclosed or divulged to can lookup; see https://github.com/digital-asset/daml/issues/10
      // and https://github.com/digital-asset/daml/issues/751 .
      Ref.Party fromString submitter exists (p => ac.witnesses(p) || ac.divulgences(p))
    }

    override def lookupActiveContract(submitter: Party, contractId: Value.AbsoluteContractId)
      : Future[Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]] =
      ledger
        .lookupContract(contractId)
        .map(_.collect {
          case ac if canSeeContract(submitter, ac) => ac.contract
        })(DEC)

    override def lookupContractKey(
        submitter: Party,
        key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] = {
      implicit val ec: ExecutionContext = DEC
      ledger.lookupKey(key).flatMap {
        // note that we need to check visibility for keys, too, otherwise we leak the existence of a non-divulged
        // contract if we return `Some`.
        case None => Future.successful(None)
        case Some(cid) =>
          ledger.lookupContract(cid) map {
            _ flatMap (ac => if (canSeeContract(submitter, ac)) Some(cid) else None)
          }
      }
    }
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

  private def toLedgerSyncEvent(offset: Long, item: LedgerEntry): LedgerSyncEvent =
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
      case t: LedgerEntry.Transaction => toAcceptedTransaction(offset, t)
      case LedgerEntry.Checkpoint(recordedAt) =>
        Heartbeat(
          recordedAt,
          offset.toString
        )
    }

  override def close(): Unit =
    ledger.close()

  private def toAcceptedTransaction(offset: Long, t: LedgerEntry.Transaction) = t match {
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
  }

  override def getTransactionById(
      transactionId: TransactionId): Future[Option[AcceptedTransaction]] =
    ledger
      .lookupTransaction(transactionId)
      .map(_.map {
        case (offset, t) =>
          toAcceptedTransaction(offset, t)
      })(DEC)

}
