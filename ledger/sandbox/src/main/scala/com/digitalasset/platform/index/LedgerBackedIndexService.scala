// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  AcsUpdateEvent,
  ActiveContractSetSnapshot,
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  CommandSubmissionResult,
  IndexService,
  PackageDetails
}
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  LedgerId,
  LedgerOffset,
  PackageEntry,
  PartyDetails,
  PartyEntry,
  TransactionFilter,
  TransactionId
}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.entries.{
  CommandDeduplicationEntry,
  LedgerEntry,
  PartyLedgerEntry
}
import com.digitalasset.platform.store.{LedgerSnapshot, ReadOnlyLedger}

import scala.concurrent.Future

abstract class LedgerBackedIndexService(
    ledger: ReadOnlyLedger,
    participantId: ParticipantId,
)(implicit mat: Materializer)
    extends IndexService {
  override def getLedgerId(): Future[LedgerId] = Future.successful(ledger.ledgerId)

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def getActiveContractSetSnapshot(
      filter: TransactionFilter): Future[ActiveContractSetSnapshot] = {
    ledger
      .snapshot(filter)
      .map {
        case LedgerSnapshot(offset, acsStream) =>
          ActiveContractSetSnapshot(
            LedgerOffset.Absolute(LedgerString.fromLong(offset)),
            acsStream
              .mapConcat { ac =>
                EventFilter(ac)(filter)
                  .map(create =>
                    create.workflowId.map(domain.WorkflowId(_)) -> toUpdateEvent(create))
                  .toList
              }
          )
      }(mat.executionContext)
  }

  private def toUpdateEvent(ac: ActiveContract): AcsUpdateEvent.Create =
    AcsUpdateEvent.Create(
      // we use absolute contract ids as event ids throughout the sandbox
      domain.TransactionId(ac.transactionId),
      domain.EventId(ac.eventId),
      ac.id,
      ac.contract.template,
      ac.contract.arg,
      ac.witnesses,
      ac.key.map(_.key),
      ac.signatories,
      ac.observers,
      ac.agreementText
    )

  private def getTransactionById(
      transactionId: TransactionId): Future[Option[(Long, LedgerEntry.Transaction)]] = {
    ledger
      .lookupTransaction(transactionId)
      .map(_.map { case (offset, t) => (offset + 1) -> t })(DEC)
  }

  override def transactionTrees(
      begin: LedgerOffset,
      endAt: Option[LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetTransactionTreesResponse, NotUsed] =
    acceptedTransactions(begin, endAt)
      .mapConcat {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToTransaction(offset, transaction, filter, verbose)
            .map(tx => GetTransactionTreesResponse(Seq(tx)))
            .toList
      }

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetTransactionsResponse, NotUsed] =
    acceptedTransactions(begin, endAt)
      .mapConcat {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToFlatTransaction(offset, transaction, filter, verbose)
            .map(tx => GetTransactionsResponse(Seq(tx)))
            .toList
      }

  private class OffsetConverter {
    lazy val currentEndF: Future[LedgerOffset.Absolute] = currentLedgerEnd()

    def toAbsolute(offset: LedgerOffset): Source[LedgerOffset.Absolute, NotUsed] = offset match {
      case LedgerOffset.LedgerBegin =>
        Source.single(LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")))
      case LedgerOffset.LedgerEnd => Source.future(currentEndF)
      case off @ LedgerOffset.Absolute(_) => Source.single(off)
    }
  }

  private def acceptedTransactions(begin: domain.LedgerOffset, endAt: Option[domain.LedgerOffset])
    : Source[(LedgerOffset.Absolute, LedgerEntry.Transaction), NotUsed] = {
    val converter = new OffsetConverter()

    converter.toAbsolute(begin).flatMapConcat {
      case LedgerOffset.Absolute(absBegin) =>
        endAt
          .map(converter.toAbsolute(_).map(Some(_)))
          .getOrElse(Source.single(None))
          .flatMapConcat { endOpt =>
            lazy val stream =
              ledger.ledgerEntries(Some(absBegin.toLong), endOpt.map(_.value.toLong))

            val finalStream = endOpt match {
              case None => stream

              case Some(LedgerOffset.Absolute(`absBegin`)) =>
                Source.empty

              case Some(LedgerOffset.Absolute(end)) if absBegin.toLong > end.toLong =>
                Source.failed(
                  ErrorFactories.invalidArgument(s"End offset $end is before Begin offset $begin."))

              case Some(LedgerOffset.Absolute(end)) =>
                val endL = end.toLong
                stream
                  .takeWhile(
                    {
                      case (offset, _) =>
                        //note that we can have gaps in the increasing offsets!
                        (offset + 1) < endL //api offsets are +1 compared to backend offsets
                    },
                    inclusive = true // we need this to be inclusive otherwise the stream will be hanging until a new element from upstream arrives
                  )
                  // after the following step, we will add +1 to the offset before we expose it on the ledger api.
                  // when the interval is [5, 7[, then we should only emit ledger entries with _backend_ offsets 5 and 6,
                  // which then get changed to 6 and 7 respectively. the application can then take the offset of the last received
                  // transaction and use it as a begin offset for another transaction service request.
                  .filter(_._1 < endL)
            }
            // we MUST do the offset comparison BEFORE collecting only the accepted transactions,
            // because currentLedgerEnd refers to the offset of the mixed set of LedgerEntries (e.g. completions, transactions, ...).
            // If we don't do this, the response stream will linger until a transaction is committed AFTER the end offset.
            // The immediate effect is that integration tests will not complete within the timeout.
            finalStream.collect {
              case (offset, t: LedgerEntry.Transaction) =>
                (LedgerOffset.Absolute(LedgerString.assertFromString((offset + 1).toString)), t)
            }
          }
    }
  }

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    Future.successful(LedgerOffset.Absolute(LedgerString.fromLong(ledger.ledgerEnd)))

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[GetFlatTransactionResponse]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToFlatTransaction(
              LedgerOffset.Absolute(LedgerString.fromLong(offset)),
              transaction,
              filter,
              verbose = true)
            .map(tx => GetFlatTransactionResponse(Option(tx)))
      })(DEC)
  }

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[GetTransactionResponse]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToTransaction(
              LedgerOffset.Absolute(LedgerString.fromLong(offset)),
              transaction,
              filter,
              verbose = true)
            .map(tx => GetTransactionResponse(Option(tx)))
      })(DEC)
  }

  override def getCompletions(
      begin: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]
  ): Source[CompletionStreamResponse, NotUsed] =
    new OffsetConverter().toAbsolute(begin).flatMapConcat {
      case LedgerOffset.Absolute(absBegin) =>
        ledger.completions(Option(absBegin.toLong), None, applicationId, parties).map(_._2)
    }

  // IndexPackagesService
  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    ledger.listLfPackages()

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    ledger.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    ledger.getLfPackage(packageId)

  override def lookupActiveContract(
      submitter: Ref.Party,
      contractId: AbsoluteContractId,
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    ledger.lookupContract(contractId, submitter)

  override def lookupContractKey(
      submitter: Party,
      key: GlobalKey,
  ): Future[Option[AbsoluteContractId]] =
    ledger.lookupKey(key, submitter)

  // PartyManagementService
  override def getParticipantId(): Future[ParticipantId] =
    Future.successful(participantId)

  override def listParties(): Future[List[PartyDetails]] =
    ledger.parties

  override def partyEntries(beginOffset: LedgerOffset.Absolute): Source[PartyEntry, NotUsed] = {
    ledger.partyEntries(beginOffset.value.toLong).map {
      case (_, PartyLedgerEntry.AllocationRejected(subId, participantId, _, reason)) =>
        PartyEntry.AllocationRejected(subId, domain.ParticipantId(participantId), reason)
      case (_, PartyLedgerEntry.AllocationAccepted(subId, participantId, _, details)) =>
        PartyEntry.AllocationAccepted(subId, domain.ParticipantId(participantId), details)
    }
  }

  override def packageEntries(beginOffset: LedgerOffset.Absolute): Source[PackageEntry, NotUsed] =
    ledger
      .packageEntries(beginOffset.value.toLong)
      .map(_._2.toDomain)

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to further configuration changes.
    * The offset is internal and not exposed over Ledger API.
    */
  override def lookupConfiguration(): Future[Option[(Long, Configuration)]] =
    ledger.lookupLedgerConfiguration()

  /** Retrieve configuration entries. */
  override def configurationEntries(
      startInclusive: Option[Long]): Source[domain.ConfigurationEntry, NotUsed] =
    ledger.configurationEntries(startInclusive).map(_._2.toDomain)

  /** Deduplicate commands */
  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      ttl: Instant): Future[CommandDeduplicationResult] =
    ledger
      .deduplicateCommand(deduplicationKey, submittedAt, ttl)
      .map {
        case None =>
          CommandDeduplicationNew
        case Some(CommandDeduplicationEntry(_, _, _, _)) =>
          CommandDeduplicationDuplicate
      }(DEC)

  override def updateCommandResult(
      deduplicationKey: String,
      submittedAt: Instant,
      result: CommandSubmissionResult): Future[Unit] =
    ledger.updateCommandResult(deduplicationKey: String, submittedAt, result)
}
