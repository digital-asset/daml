// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  ActiveContractSetSnapshot,
  ActiveContractsService,
  TransactionsService
}
import com.daml.ledger.participant.state.{v1 => ParticipantState}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party, TransactionIdString}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{EventId, LedgerOffset, TransactionFilter, TransactionId}
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.{
  AcceptedTransaction,
  Heartbeat,
  RejectedCommand
}
import com.digitalasset.ledger.backend.api.v1._
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.sandbox.stores.ActiveContracts
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.services.transaction.TransactionConversion

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scalaz.syntax.tag._

class SandboxLedgerBackend(ledger: Ledger)(implicit mat: Materializer)
    extends LedgerBackend //TODO: remove this later so we can rely on sole participant state interfaces
    with ParticipantState.WriteService
    with ActiveContractsService
    with TransactionsService {

  private class SandboxSubmissionHandle extends SubmissionHandle {
    override def abort: Future[Unit] = Future.successful(())

    override def submit(
        submitted: TransactionSubmission): Future[ParticipantState.SubmissionResult] =
      ledger.publishTransaction(submitted)

    private[this] def canSeeContract(
        submitter: Party,
        ac: ActiveContracts.ActiveContract): Boolean = {
      // ^ only parties disclosed or divulged to can lookup; see https://github.com/digital-asset/daml/issues/10
      // and https://github.com/digital-asset/daml/issues/751 .
      Party fromString submitter exists (p => ac.witnesses(p) || ac.divulgences.contains(p))
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

  override def getActiveContractSetSnapshot(
      filter: TransactionFilter): Future[ActiveContractSetSnapshot] =
    ledger
      .snapshot()
      .map {
        case LedgerSnapshot(offset, acsStream) =>
          ActiveContractSetSnapshot(
            LedgerOffset.Absolute(LedgerString.fromLong(offset)),
            acsStream
              .mapConcat {
                case (cId, ac) =>
                  val create = toUpdateEvent(cId, ac)
                  EventFilter
                    .byTemplates(filter)
                    .filterActiveContractWitnesses(create)
                    .map(create => ac.workflowId.map(domain.WorkflowId(_)) -> create)
                    .toList
              }
          )
      }(mat.executionContext)

  override def getCurrentLedgerEnd: Future[LedgerSyncOffset] =
    Future.successful(LedgerString.fromLong(ledger.ledgerEnd))

  private def toUpdateEvent(
      cId: Value.AbsoluteContractId,
      ac: ActiveContracts.ActiveContract): AcsUpdateEvent.Create =
    AcsUpdateEvent.Create(
      // we use absolute contract ids as event ids throughout the sandbox
      domain.TransactionId(ac.transactionId),
      EventId(cId.coid),
      cId,
      ac.contract.template,
      ac.contract.arg,
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
          LedgerString.fromLong(offset),
          Some(applicationId))
      case t: LedgerEntry.Transaction => toAcceptedTransaction(offset, t)
      case LedgerEntry.Checkpoint(recordedAt) =>
        Heartbeat(
          recordedAt,
          LedgerString.fromLong(offset)
        )
    }

  override def close(): Unit = {} // nothing to close here as we do not own Ledger

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
        LedgerString.fromLong(offset),
        workflowId,
        explicitDisclosure,
        Some(applicationId),
        Some(commandId)
      )
  }

  override def getTransactionById(
      transactionId: TransactionIdString): Future[Option[AcceptedTransaction]] =
    ledger
      .lookupTransaction(transactionId)
      .map(_.map {
        case (offset, t) =>
          increaseOffset(toAcceptedTransaction(offset, t))
      })(DEC)

  override def submitTransaction(
      submitterInfo: ParticipantState.SubmitterInfo,
      transactionMeta: ParticipantState.TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[ParticipantState.SubmissionResult] = {

    implicit val ec: ExecutionContext = mat.executionContext

    //note, that this cannot fail as it's already validated
    val blindingInfo = Blinding
      .checkAuthorizationAndBlind(transaction, Set(submitterInfo.submitter))
      .fold(authorisationError => sys.error(authorisationError.detailMsg), identity)

    val transactionSubmission =
      TransactionSubmission(
        submitterInfo.commandId,
        transactionMeta.workflowId,
        submitterInfo.submitter,
        transactionMeta.ledgerEffectiveTime.toInstant,
        submitterInfo.maxRecordTime.toInstant,
        submitterInfo.applicationId,
        blindingInfo,
        transaction
      )

    val resultF = for {
      handle <- beginSubmission()
      result <- handle.submit(transactionSubmission)
    } yield result

    FutureConverters.toJava(resultF)
  }

  override def transactionTrees(
      begin: LedgerOffset,
      endAt: Option[LedgerOffset],
      filter: domain.TransactionFilter): Source[domain.TransactionTree, NotUsed] = {
    acceptedTransactions(begin, endAt)
      .mapConcat(TransactionConversion.acceptedToDomainTree(_, filter).toList)
  }

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter): Source[domain.Transaction, NotUsed] = {
    acceptedTransactions(begin, endAt)
      .mapConcat(TransactionConversion.acceptedToDomainFlat(_, filter).toList)
  }

  private def acceptedTransactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset]): Source[AcceptedTransaction, NotUsed] = {
    lazy val currentEndF = currentLedgerEnd()

    def toAbsolute(offset: LedgerOffset) = offset match {
      case LedgerOffset.LedgerBegin =>
        Source.single(LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")))
      case LedgerOffset.LedgerEnd => Source.fromFuture(currentEndF)
      case off @ LedgerOffset.Absolute(_) => Source.single(off)
    }

    toAbsolute(begin).flatMapConcat {
      case LedgerOffset.Absolute(absBegin) =>
        endAt
          .map(toAbsolute(_).map(Some(_)))
          .getOrElse(Source.single(None))
          .flatMapConcat { endOpt =>
            lazy val stream = ledgerSyncEvents(Some(absBegin))

            val finalStream = endOpt match {
              case None => stream

              case Some(LedgerOffset.Absolute(`absBegin`)) =>
                Source.empty

              case Some(LedgerOffset.Absolute(end)) if absBegin.toLong > end.toLong =>
                Source.failed(
                  ErrorFactories.invalidArgument(s"End offset $end is before Begin offset $begin."))

              case Some(LedgerOffset.Absolute(end)) =>
                stream
                  .takeWhile(
                    { item =>
                      //note that we can have gaps in the increasing offsets!
                      (item.offset.toLong + 1) < end.toLong //api offsets are +1 compared to backend offsets
                    },
                    inclusive = true // we need this to be inclusive otherwise the stream will be hanging until a new element from upstream arrives
                  )
                  .filter(_.offset.toLong < end.toLong)
            }
            // we MUST do the offset comparison BEFORE collecting only the accepted transactions,
            // because currentLedgerEnd refers to the offset of the mixed set of LedgerSyncEvents (e.g. completions, transactions, ...).
            // If we don't do this, the response stream will linger until a transaction is committed AFTER the end offset.
            // The immediate effect is that integration tests will not complete within the timeout.
            finalStream.collect {
              case at: LedgerSyncEvent.AcceptedTransaction => increaseOffset(at)
            }
          }
    }
  }

  private def increaseOffset(t: AcceptedTransaction) = {
    t.copy(offset = LedgerString.fromLong(t.offset.toLong + 1))
  }

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    getCurrentLedgerEnd.map(LedgerOffset.Absolute)(DEC)
  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[domain.Transaction]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId.unwrap)
      .map(_.flatMap(TransactionConversion.acceptedToDomainFlat(_, filter)))(DEC)
  }

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[domain.TransactionTree]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId.unwrap)
      .map(_.flatMap(TransactionConversion.acceptedToDomainTree(_, filter)))(DEC)
  }
}
