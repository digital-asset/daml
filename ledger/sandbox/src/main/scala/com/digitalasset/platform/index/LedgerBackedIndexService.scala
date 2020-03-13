// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{Configuration, Offset, ParticipantId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
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
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.entries.{LedgerEntry, PartyLedgerEntry}
import com.digitalasset.platform.store.{LedgerSnapshot, ReadOnlyLedger}
import com.digitalasset.platform.ApiOffset.ApiOffsetConverter

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
            toAbsolute(offset),
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
      transactionId: TransactionId): Future[Option[(Offset, LedgerEntry.Transaction)]] = {
    ledger
      .lookupTransaction(transactionId)
  }

  override def transactionTrees(
      startExclusive: LedgerOffset,
      endInclusive: Option[LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetTransactionTreesResponse, NotUsed] =
    acceptedTransactions(startExclusive, endInclusive)
      .mapConcat {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToTransactionTree(
              offset,
              transaction,
              filter.filtersByParty.keySet,
              verbose)
            .map(tx => GetTransactionTreesResponse(Seq(tx)))
            .toList
      }

  override def transactions(
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetTransactionsResponse, NotUsed] =
    acceptedTransactions(startExclusive, endInclusive)
      .mapConcat {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToFlatTransaction(offset, transaction, filter, verbose)
            .map(tx => GetTransactionsResponse(Seq(tx)))
            .toList
      }

  private class OffsetConverter {
    lazy val currentEnd: Offset = ledger.ledgerEnd

    def toAbsolute(offset: LedgerOffset): Source[Offset, NotUsed] = offset match {
      case LedgerOffset.LedgerBegin => Source.single(Offset.empty)
      case LedgerOffset.LedgerEnd => Source.single(currentEnd)
      case LedgerOffset.Absolute(offset) =>
        ApiOffset.fromString(offset).fold(Source.failed, off => Source.single(off))
    }
  }

  private def acceptedTransactions(
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset])
    : Source[(LedgerOffset.Absolute, LedgerEntry.Transaction), NotUsed] = {
    val converter = new OffsetConverter()

    converter.toAbsolute(startExclusive).flatMapConcat { begin =>
      endInclusive
        .map(converter.toAbsolute(_).map(Some(_)))
        .getOrElse(Source.single(None))
        .flatMapConcat {
          case Some(`begin`) =>
            Source.empty

          case Some(end) if begin > end =>
            Source.failed(ErrorFactories.invalidArgument(
              s"End offset ${end.toApiString} is before Begin offset ${begin.toApiString}."))

          case endOpt @ (None | Some(_)) =>
            ledger
              .ledgerEntries(Some(begin), endOpt)
              .collect {
                case (offset, t: LedgerEntry.Transaction) =>
                  (toAbsolute(offset), t)
              }
        }
    }
  }

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    Future.successful(toAbsolute(ledger.ledgerEnd))

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[GetFlatTransactionResponse]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion
            .ledgerEntryToFlatTransaction(toAbsolute(offset), transaction, filter, verbose = true)
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
            .ledgerEntryToTransactionTree(
              toAbsolute(offset),
              transaction,
              filter.filtersByParty.keySet,
              verbose = true)
            .map(tx => GetTransactionResponse(Option(tx)))
      })(DEC)
  }

  def toAbsolute(offset: Offset): LedgerOffset.Absolute =
    LedgerOffset.Absolute(ApiOffset.toApiString(offset))

  override def getCompletions(
      startExclusive: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]
  ): Source[CompletionStreamResponse, NotUsed] =
    new OffsetConverter().toAbsolute(startExclusive).flatMapConcat { beginOpt =>
      ledger.completions(Some(beginOpt), None, applicationId, parties).map(_._2)
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

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    ledger.getParties(parties)

  override def listKnownParties(): Future[List[PartyDetails]] =
    ledger.listKnownParties()

  override def partyEntries(startExclusive: LedgerOffset.Absolute): Source[PartyEntry, NotUsed] = {
    Source
      .future(Future.fromTry(ApiOffset.fromString(startExclusive.value)))
      .flatMapConcat(ledger.partyEntries)
      .map {
        case (_, PartyLedgerEntry.AllocationRejected(subId, participantId, _, reason)) =>
          PartyEntry.AllocationRejected(subId, domain.ParticipantId(participantId), reason)
        case (_, PartyLedgerEntry.AllocationAccepted(subId, participantId, _, details)) =>
          PartyEntry.AllocationAccepted(subId, domain.ParticipantId(participantId), details)
      }
  }

  override def packageEntries(
      startExclusive: LedgerOffset.Absolute): Source[PackageEntry, NotUsed] =
    Source
      .future(Future.fromTry(ApiOffset.fromString(startExclusive.value)))
      .flatMapConcat(ledger.packageEntries)
      .map(_._2.toDomain)

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to further configuration changes.
    * The offset is internal and not exposed over Ledger API.
    */
  override def lookupConfiguration(): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    ledger
      .lookupLedgerConfiguration()
      .map(_.map { case (offset, config) => (toAbsolute(offset), config) })(DEC)

  /** Retrieve configuration entries. */
  override def configurationEntries(
      startExclusive: Option[LedgerOffset.Absolute]): Source[domain.ConfigurationEntry, NotUsed] =
    Source
      .future(
        startExclusive
          .map(off => Future.fromTry(ApiOffset.fromString(off.value).map(Some(_))))
          .getOrElse(Future.successful(None)))
      .flatMapConcat(ledger.configurationEntries(_).map(_._2.toDomain))

  /** Deduplicate commands */
  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    ledger.deduplicateCommand(deduplicationKey, submittedAt, deduplicateUntil)
}
