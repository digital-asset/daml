// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{IndexPackagesService, _}
import com.daml.ledger.participant.state.{v1 => ParticipantState}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, TransactionIdString}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.CompletionEvent.{
  Checkpoint,
  CommandAccepted,
  CommandRejected
}
import com.digitalasset.ledger.api.domain.RejectionReason._
import com.digitalasset.ledger.api.domain.{LedgerId, _}
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.{
  AcceptedTransaction,
  Heartbeat,
  RejectedCommand
}
import com.digitalasset.ledger.backend.api.v1.{ApplicationId => _, RejectionReason => _, _}
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.sandbox.stores.ActiveContracts
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.services.transaction.TransactionConversion
import com.digitalasset.platform.services.time.TimeModel
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future, Promise}

class SandboxIndexService(ledger: Ledger, timeModel: TimeModel, templateStore: IndexPackagesService)(
    implicit mat: Materializer)
    extends ParticipantState.WriteService
    with IdentityProvider
    with IndexActiveContractsService
    with IndexTransactionsService
    with IndexCompletionsService
    with IndexConfigurationService
    with IndexPackagesService {

  override def getLedgerId(): Future[LedgerId] = Future.successful(ledger.ledgerId)

  override def getLedgerConfiguration(): Source[LedgerConfiguration, NotUsed] =
    Source
      .single(LedgerConfiguration(timeModel.minTtl, timeModel.maxTtl))
      .concat(Source.fromFuture(Promise[LedgerConfiguration]().future)) // we should keep the stream open!

  private def ledgerSyncEvents(offset: Option[LedgerSyncOffset]): Source[LedgerSyncEvent, NotUsed] =
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

  private def getTransactionById(
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

    val resultF = ledger.publishTransaction(transactionSubmission)

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

  private class OffsetConverter {
    lazy val currentEndF = currentLedgerEnd()

    def toAbsolute(offset: LedgerOffset) = offset match {
      case LedgerOffset.LedgerBegin =>
        Source.single(LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")))
      case LedgerOffset.LedgerEnd => Source.fromFuture(currentEndF)
      case off @ LedgerOffset.Absolute(_) => Source.single(off)
    }
  }

  private def acceptedTransactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset]): Source[AcceptedTransaction, NotUsed] = {
    val converter = new OffsetConverter()

    converter.toAbsolute(begin).flatMapConcat {
      case LedgerOffset.Absolute(absBegin) =>
        endAt
          .map(converter.toAbsolute(_).map(Some(_)))
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

  private def increaseOffset(t: AcceptedTransaction) =
    t.copy(offset = LedgerString.fromLong(t.offset.toLong + 1))

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    Future.successful(LedgerOffset.Absolute(LedgerString.fromLong(ledger.ledgerEnd)))

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

  override def getCompletions(
      begin: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]
  ): Source[CompletionEvent, NotUsed] = {
    val converter = new OffsetConverter()
    converter.toAbsolute(begin).flatMapConcat {
      case LedgerOffset.Absolute(absBegin) =>
        ledgerSyncEvents(Some(absBegin)).collect {
          case LedgerSyncEvent
                .AcceptedTransaction(
                _,
                transactionId,
                Some(submitter),
                _,
                recordTime,
                offset,
                _,
                _,
                Some(appId),
                Some(commandId))
              if (appId == applicationId.unwrap && parties.contains(submitter)) =>
            CommandAccepted(
              domain.LedgerOffset.Absolute(offset),
              recordTime,
              domain.CommandId(commandId),
              domain.TransactionId(transactionId))
          case hb: LedgerSyncEvent.Heartbeat =>
            Checkpoint(domain.LedgerOffset.Absolute(hb.offset), hb.recordTime)
          case rc: LedgerSyncEvent.RejectedCommand =>
            CommandRejected(
              domain.LedgerOffset.Absolute(rc.offset),
              rc.recordTime,
              domain.CommandId(rc.commandId),
              convertRejectionReason(rc.rejectionReason))
        }
    }
  }

  import com.digitalasset.ledger._

  private def convertRejectionReason(rr: backend.api.v1.RejectionReason): RejectionReason =
    rr match {
      case _: backend.api.v1.RejectionReason.Inconsistent =>
        Inconsistent(rr.description)
      case _: backend.api.v1.RejectionReason.OutOfQuota =>
        OutOfQuota(rr.description)
      case _: backend.api.v1.RejectionReason.TimedOut =>
        TimedOut(rr.description)
      case _: backend.api.v1.RejectionReason.Disputed =>
        Disputed(rr.description)
      case _: backend.api.v1.RejectionReason.DuplicateCommandId =>
        DuplicateCommandId(rr.description)
    }

  override def listPackages(): Future[Set[PackageId]] =
    templateStore.listPackages()

  override def getPackage(packageId: PackageId): Future[Option[Archive]] =
    templateStore.getPackage(packageId)

}
