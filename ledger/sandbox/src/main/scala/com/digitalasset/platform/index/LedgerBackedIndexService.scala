// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  AcsUpdateEvent,
  ActiveContractSetSnapshot,
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
import com.digitalasset.ledger.api.domain.CompletionEvent.{
  Checkpoint,
  CommandAccepted,
  CommandRejected
}
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  CompletionEvent,
  LedgerId,
  LedgerOffset,
  PackageEntry,
  PartyDetails,
  PartyEntry,
  TransactionFilter,
  TransactionId
}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.entries.{LedgerEntry, PartyLedgerEntry}
import com.digitalasset.platform.store.{LedgerSnapshot, ReadOnlyLedger}
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.Future

abstract class LedgerBackedIndexService(
    ledger: ReadOnlyLedger,
    participantId: ParticipantId,
)(implicit mat: Materializer)
    extends IndexService {
  override def getLedgerId(): Future[LedgerId] = Future.successful(ledger.ledgerId)

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def getActiveContractSetSnapshot(
      txFilter: TransactionFilter): Future[ActiveContractSetSnapshot] = {
    val filter = EventFilter.byTemplates(txFilter)
    ledger
      .snapshot(filter)
      .map {
        case LedgerSnapshot(offset, acsStream) =>
          ActiveContractSetSnapshot(
            LedgerOffset.Absolute(LedgerString.fromLong(offset)),
            acsStream
              .mapConcat { ac =>
                val create = toUpdateEvent(ac.id, ac)
                EventFilter
                  .filterActiveContractWitnesses(filter, create)
                  .map(create => ac.workflowId.map(domain.WorkflowId(_)) -> create)
                  .toList
              }
          )
      }(mat.executionContext)
  }

  private def toUpdateEvent(
      cId: Value.AbsoluteContractId,
      ac: ActiveContract
  ): AcsUpdateEvent.Create =
    AcsUpdateEvent.Create(
      // we use absolute contract ids as event ids throughout the sandbox
      domain.TransactionId(ac.transactionId),
      domain.EventId(ac.eventId),
      cId,
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
      filter: domain.TransactionFilter): Source[domain.TransactionTree, NotUsed] =
    acceptedTransactions(begin, endAt)
      .mapConcat {
        case (offset, transaction) =>
          TransactionConversion.ledgerEntryToDomainTree(offset, transaction, filter).toList
      }

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter): Source[domain.Transaction, NotUsed] =
    acceptedTransactions(begin, endAt)
      .mapConcat {
        case (offset, transaction) =>
          TransactionConversion.ledgerEntryToDomainFlat(offset, transaction, filter).toList
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
      requestingParties: Set[Ref.Party]): Future[Option[domain.Transaction]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion.ledgerEntryToDomainFlat(
            LedgerOffset.Absolute(LedgerString.fromLong(offset)),
            transaction,
            filter)
      })(DEC)
  }

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[domain.TransactionTree]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion.ledgerEntryToDomainTree(
            LedgerOffset.Absolute(LedgerString.fromLong(offset)),
            transaction,
            filter)
      })(DEC)
  }

  override def getCompletions(
      begin: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]
  ): Source[CompletionEvent, NotUsed] = {
    val converter = new OffsetConverter()
    converter.toAbsolute(begin).flatMapConcat {
      case LedgerOffset.Absolute(absBegin) =>
        ledger
          .ledgerEntries(Some(absBegin.toLong), endExclusive = None)
          .map {
            case (offset, entry) =>
              (offset + 1, entry) //doing the same as above with transactions. The ledger api has to return a non-inclusive offset
          }
          .collect {
            case (offset, t: LedgerEntry.Transaction)
                // We only send out completions for transactions for which we have the full submitter information (appId, submitter, cmdId).
                //
                // This doesn't make a difference for the sandbox (because it represents the ledger backend + api server in single package).
                // But for an api server that is part of a distributed ledger network, we might see
                // transactions that originated from some other api server. These transactions don't contain the submitter information,
                // and therefore we don't emit CommandAccepted completions for those
                if t.applicationId.contains(applicationId.unwrap) &&
                  t.submittingParty.exists(parties.contains) &&
                  t.commandId.nonEmpty =>
              CommandAccepted(
                domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString(offset.toString)),
                t.recordedAt,
                Tag.subst(t.commandId).get,
                domain.TransactionId(t.transactionId)
              )

            case (offset, c: LedgerEntry.Checkpoint) =>
              Checkpoint(
                domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString(offset.toString)),
                c.recordedAt)
            case (offset, r: LedgerEntry.Rejection)
                if r.commandId.nonEmpty && r.applicationId.contains(applicationId.unwrap) =>
              CommandRejected(
                domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString(offset.toString)),
                r.recordTime,
                domain.CommandId(r.commandId),
                r.rejectionReason)
          }
    }
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
}
