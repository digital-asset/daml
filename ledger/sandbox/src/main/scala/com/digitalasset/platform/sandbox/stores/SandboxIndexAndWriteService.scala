// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{
  ApplicationId => _,
  LedgerId => _,
  TransactionId => _,
  _
}
import com.daml.ledger.participant.state.{v1 => ParticipantState}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party, TransactionIdString}
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.CompletionEvent.{
  Checkpoint,
  CommandAccepted,
  CommandRejected
}
import com.digitalasset.ledger.api.domain.{ParticipantId => _, _}
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger._
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.services.time.TimeModel
import org.slf4j.LoggerFactory
import scalaz.Tag
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

trait IndexAndWriteService extends AutoCloseable {
  def indexService: IndexService

  def writeService: WriteService

  def publishHeartbeat(instant: Instant): Future[Unit]
}

object SandboxIndexAndWriteService {
  //TODO: internalise the template store as well
  private val logger = LoggerFactory.getLogger(SandboxIndexAndWriteService.getClass)

  def postgres(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      startMode: SqlStartMode,
      queueDepth: Int,
      templateStore: InMemoryPackageStore)(
      implicit mat: Materializer,
      mm: MetricsManager): Future[IndexAndWriteService] =
    Ledger
      .jdbcBacked(
        jdbcUrl,
        ledgerId,
        timeProvider,
        acs,
        templateStore,
        ledgerEntries,
        queueDepth,
        startMode
      )
      .map(ledger =>
        createInstance(Ledger.metered(ledger), participantId, timeModel, timeProvider))(DEC)

  def inMemory(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      templateStore: InMemoryPackageStore)(
      implicit mat: Materializer,
      mm: MetricsManager): IndexAndWriteService = {
    val ledger =
      Ledger.metered(Ledger.inMemory(ledgerId, timeProvider, acs, templateStore, ledgerEntries))
    createInstance(ledger, participantId, timeModel, timeProvider)
  }

  private def createInstance(
      ledger: Ledger,
      participantId: ParticipantId,
      timeModel: TimeModel,
      timeProvider: TimeProvider)(implicit mat: Materializer) = {
    val contractStore = new SandboxContractStore(ledger)
    val indexSvc = new LedgerBackedIndexService(ledger, contractStore, participantId) {
      override def getLedgerConfiguration(): Source[LedgerConfiguration, NotUsed] =
        Source
          .single(LedgerConfiguration(timeModel.minTtl, timeModel.maxTtl))
          .concat(Source.fromFuture(Promise[LedgerConfiguration]().future)) // we should keep the stream open!
    }
    val writeSvc = new LedgerBackedWriteService(ledger, timeProvider)
    val heartbeats = scheduleHeartbeats(timeProvider, ledger.publishHeartbeat)

    new IndexAndWriteService {
      override def indexService: IndexService = indexSvc

      override def writeService: WriteService = writeSvc

      override def publishHeartbeat(instant: Instant): Future[Unit] =
        ledger.publishHeartbeat(instant)

      override def close(): Unit = {
        heartbeats.close()
        ledger.close()
      }
    }
  }

  private def scheduleHeartbeats(timeProvider: TimeProvider, onTimeChange: Instant => Future[Unit])(
      implicit mat: Materializer): AutoCloseable =
    timeProvider match {
      case timeProvider: TimeProvider.UTC.type =>
        val interval = 1.seconds
        logger.debug(s"Scheduling heartbeats in intervals of {}", interval)
        val cancelable = Source
          .tick(0.seconds, interval, ())
          .mapAsync[Unit](1)(
            _ => onTimeChange(timeProvider.getCurrentTime)
          )
          .to(Sink.ignore)
          .run()
        () =>
          val _ = cancelable.cancel()
      case _ =>
        () =>
          ()
    }
}

abstract class LedgerBackedIndexService(
    ledger: ReadOnlyLedger,
    contractStore: ContractStore,
    participantId: ParticipantId
)(implicit mat: Materializer)
    extends IndexService
    with AutoCloseable {
  override def getLedgerId(): Future[LedgerId] = Future.successful(ledger.ledgerId)

  override def getActiveContractSetSnapshot(
      filter: TransactionFilter): Future[ActiveContractSetSnapshot] =
    ledger
      .snapshot()
      .map {
        case LedgerSnapshot(offset, acsStream) =>
          ActiveContractSetSnapshot(
            LedgerOffset.Absolute(LedgerString.fromLong(offset)),
            acsStream
              .mapConcat { ac =>
                val create = toUpdateEvent(ac.id, ac)
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
      ac: ActiveLedgerState.ActiveContract): AcsUpdateEvent.Create =
    AcsUpdateEvent.Create(
      // we use absolute contract ids as event ids throughout the sandbox
      domain.TransactionId(ac.transactionId),
      EventId(cId.coid),
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
      transactionId: TransactionIdString): Future[Option[(Long, LedgerEntry.Transaction)]] = {
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
    lazy val currentEndF = currentLedgerEnd()

    def toAbsolute(offset: LedgerOffset) = offset match {
      case LedgerOffset.LedgerBegin =>
        Source.single(LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")))
      case LedgerOffset.LedgerEnd => Source.fromFuture(currentEndF)
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
            lazy val stream = ledger.ledgerEntries(Some(absBegin.toLong))

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
                    {
                      case (offset, _) =>
                        //note that we can have gaps in the increasing offsets!
                        (offset + 1) < end.toLong //api offsets are +1 compared to backend offsets
                    },
                    inclusive = true // we need this to be inclusive otherwise the stream will be hanging until a new element from upstream arrives
                  )
                  .filter(_._1 < end.toLong)
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
    getTransactionById(transactionId.unwrap)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion.ledgerEntryToDomainFlat(
            LedgerOffset.Absolute(LedgerString.assertFromString(offset.toString)),
            transaction,
            filter)
      })(DEC)
  }

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]): Future[Option[domain.TransactionTree]] = {
    val filter =
      domain.TransactionFilter(requestingParties.map(p => p -> domain.Filters.noFilter).toMap)
    getTransactionById(transactionId.unwrap)
      .map(_.flatMap {
        case (offset, transaction) =>
          TransactionConversion.ledgerEntryToDomainTree(
            LedgerOffset.Absolute(LedgerString.assertFromString(offset.toString)),
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
          .ledgerEntries(Some(absBegin.toLong))
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

  // ContractStore
  override def lookupActiveContract(
      submitter: Ref.Party,
      contractId: AbsoluteContractId
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    contractStore.lookupActiveContract(submitter, contractId)

  override def lookupContractKey(
      submitter: Party,
      key: GlobalKey): Future[Option[AbsoluteContractId]] =
    contractStore.lookupContractKey(submitter, key)

  // PartyManagementService
  override def getParticipantId(): Future[ParticipantId] =
    Future.successful(participantId)

  override def listParties(): Future[List[PartyDetails]] =
    ledger.parties

  override def close(): Unit = {
    ledger.close()
  }
}

class LedgerBackedWriteService(ledger: Ledger, timeProvider: TimeProvider) extends WriteService {

  override def submitTransaction(
      submitterInfo: ParticipantState.SubmitterInfo,
      transactionMeta: ParticipantState.TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[ParticipantState.SubmissionResult] =
    FutureConverters.toJava(ledger.publishTransaction(submitterInfo, transactionMeta, transaction))

  override def allocateParty(
      hint: Option[String],
      displayName: Option[String]): CompletionStage[PartyAllocationResult] = {
    // In the sandbox, the hint is used as-is.
    // If hint is not a valid and unallocated party name, the call fails
    hint.map(p => Party.fromString(p)) match {
      case None =>
        FutureConverters.toJava(
          ledger.allocateParty(PartyIdGenerator.generateRandomId(), displayName))
      case Some(Right(party)) => FutureConverters.toJava(ledger.allocateParty(party, displayName))
      case Some(Left(error)) =>
        CompletableFuture.completedFuture(PartyAllocationResult.InvalidName(error))
    }
  }

  // WritePackagesService
  override def uploadPackages(
      payload: List[Archive],
      sourceDescription: Option[String]
  ): CompletionStage[UploadPackagesResult] =
    FutureConverters.toJava(
      ledger.uploadPackages(timeProvider.getCurrentTime, sourceDescription, payload))

  // WriteConfigService
  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): CompletionStage[SubmissionResult] =
    // FIXME(JM): Implement configuration changes in sandbox.
    CompletableFuture.completedFuture(SubmissionResult.NotSupported)
}
