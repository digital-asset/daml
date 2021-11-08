// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.inmemory

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  Filters,
  InclusiveFilters,
  LedgerId,
  LedgerOffset,
  PartyDetails,
  RejectionReason,
  TransactionFilter,
}
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  PackageDetails,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, Result, ResultDone, ValueEnricher}
import com.daml.lf.language.Ast
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.{
  CommittedTransaction,
  GlobalKey,
  SubmittedTransaction,
  TransactionCommitter,
}
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.index.TransactionConversion
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger.inmemory.InMemoryLedger._
import com.daml.platform.sandbox.stores.ledger.{Ledger, Rejection}
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.Contract.ActiveContract
import com.daml.platform.store.Conversions.RejectionReasonOps
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}
import com.daml.platform.{ApiOffset, index}
import io.grpc.Status
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future
import scala.util.Try

/** This stores all the mutable data that we need to run a ledger: the PCS, the ACS, and the deduplicator.
  */
private[sandbox] final class InMemoryLedger(
    val ledgerId: LedgerId,
    timeProvider: TimeProvider,
    acs0: InMemoryActiveLedgerState,
    transactionCommitter: TransactionCommitter,
    packageStoreInit: InMemoryPackageStore,
    ledgerEntries: ImmArray[LedgerEntryOrBump],
    engine: Engine,
) extends Ledger {

  private val enricher = new ValueEnricher(engine)

  private def consumeEnricherResult[V](res: Result[V]): V = {
    LfEngineToApi.assertOrRuntimeEx(
      "unexpected engine.Result when enriching value",
      res match {
        case ResultDone(x) => Right(x)
        case x => Left(x.toString)
      },
    )
  }

  private def enrichTX(tx: LedgerEntry.Transaction): LedgerEntry.Transaction =
    tx.copy(transaction =
      CommittedTransaction(
        consumeEnricherResult(enricher.enrichVersionedTransaction(tx.transaction))
      )
    )

  private val logger = ContextualizedLogger.get(this.getClass)

  private val entries = {
    val l = new LedgerEntries[InMemoryEntry](_.toString)
    ledgerEntries.foreach {
      case LedgerEntryOrBump.Bump(increment) =>
        l.incrementOffset(increment)
        ()
      case LedgerEntryOrBump.Entry(entry) =>
        l.publish(InMemoryLedgerEntry(entry))
        ()
    }
    l
  }

  private val packageStoreRef = new AtomicReference[InMemoryPackageStore](packageStoreInit)

  override def currentHealth(): HealthStatus = Healthy

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .flatMapConcat {
        case (offset, InMemoryLedgerEntry(tx: LedgerEntry.Transaction)) =>
          Source(
            TransactionConversion
              .ledgerEntryToFlatTransaction(
                LedgerOffset.Absolute(ApiOffset.toApiString(offset)),
                if (verbose) { enrichTX(tx) }
                else { tx },
                TransactionFilter(filter.map { case (party, templates) =>
                  party -> Filters(
                    if (templates.nonEmpty) Some(InclusiveFilters(templates)) else None
                  )
                }),
                verbose,
              )
              .map(tx => offset -> GetTransactionsResponse(Seq(tx)))
              .toList
          )
        case _ =>
          Source.empty
      }

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Ref.Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .flatMapConcat {
        case (offset, InMemoryLedgerEntry(tx: LedgerEntry.Transaction)) =>
          Source(
            TransactionConversion
              .ledgerEntryToTransactionTree(
                LedgerOffset.Absolute(ApiOffset.toApiString(offset)),
                if (verbose) { enrichTX(tx) }
                else tx,
                requestingParties,
                verbose,
              )
              .map(tx => offset -> GetTransactionTreesResponse(Seq(tx)))
              .toList
          )
        case _ =>
          Source.empty
      }

  // mutable state
  private var acs = acs0
  private var ledgerConfiguration: Option[Configuration] = None
  private val commands: scala.collection.mutable.Map[String, CommandDeduplicationEntry] =
    scala.collection.mutable.Map.empty

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    val appId = applicationId.unwrap
    entries.getSource(startExclusive, endInclusive).collect {
      case (
            offset,
            InMemoryLedgerEntry(
              LedgerEntry.Transaction(
                Some(commandId),
                transactionId,
                Some(`appId`),
                submissionId,
                actAs,
                _,
                _,
                recordTime,
                _,
                _,
              )
            ),
          ) if actAs.exists(parties) =>
        offset -> CompletionFromTransaction.acceptedCompletion(
          recordTime,
          offset,
          commandId,
          transactionId,
          appId,
          submissionId,
        )

      case (
            offset,
            InMemoryLedgerEntry(
              LedgerEntry.Rejection(recordTime, commandId, `appId`, submissionId, actAs, reason)
            ),
          ) if actAs.exists(parties) =>
        val status = reason.toParticipantStateRejectionReason.status
        offset -> CompletionFromTransaction.rejectedCompletion(
          recordTime,
          offset,
          commandId,
          status,
          appId,
          submissionId,
        )
    }
  }

  override def ledgerEnd()(implicit loggingContext: LoggingContext): Offset = entries.ledgerEnd

  override def activeContracts(
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): (Source[GetActiveContractsResponse, NotUsed], Offset) = {
    val (acsNow, ledgerEndNow) = this.synchronized { (acs, ledgerEnd()) }
    (
      Source
        .fromIterator[ActiveContract](() =>
          acsNow.activeContracts.valuesIterator.flatMap(
            index
              .EventFilter(_)(TransactionFilter(filter.map { case (party, templates) =>
                party -> Filters(
                  if (templates.nonEmpty) Some(InclusiveFilters(templates)) else None
                )
              }))
              .toList
          )
        )
        .map { contract =>
          val contractInst =
            if (verbose) {
              consumeEnricherResult(enricher.enrichContract(contract.contract.coinst))
            } else {
              contract.contract.coinst
            }
          val contractKey = contract.key.map { key =>
            val unversionedKey = key.map(_.value)
            if (verbose) {
              consumeEnricherResult(
                enricher.enrichContractKey(contract.contract.template, unversionedKey)
              )
            } else {
              unversionedKey
            }
          }
          GetActiveContractsResponse(
            workflowId = contract.workflowId.getOrElse(""),
            activeContracts = List(
              CreatedEvent(
                EventId(contract.transactionId, contract.nodeId).toLedgerString,
                contract.id.coid,
                Some(LfEngineToApi.toApiIdentifier(contract.contract.template)),
                contractKey = contractKey.map(ck =>
                  LfEngineToApi.assertOrRuntimeEx(
                    "converting stored contract",
                    LfEngineToApi
                      .lfContractKeyToApiValue(verbose = verbose, ck),
                  )
                ),
                createArguments = Some(
                  LfEngineToApi.assertOrRuntimeEx(
                    "converting stored contract",
                    LfEngineToApi
                      .lfValueToApiRecord(verbose = verbose, contractInst.arg),
                  )
                ),
                contract.signatories.union(contract.observers).intersect(filter.keySet).toSeq,
                signatories = contract.signatories.toSeq,
                observers = contract.observers.toSeq,
                agreementText = Some(contract.agreementText),
              )
            ),
          )
        },
      ledgerEndNow,
    )
  }

  override def lookupContract(
      contractId: ContractId,
      forParties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[VersionedContractInstance]] =
    Future.successful(this.synchronized {
      acs.activeContracts
        .get(contractId)
        .filter(ac => acs.isVisibleForDivulgees(ac.id, forParties))
        .map(_.contract)
    })

  override def lookupKey(key: GlobalKey, forParties: Set[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    Future.successful(this.synchronized {
      acs.keys.get(key).filter(acs.isVisibleForStakeholders(_, forParties))
    })

  override def lookupMaximumLedgerTime(contractIds: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Timestamp]] =
    Future.fromTry(Try(this.synchronized {
      contractIds
        .foldLeft[Option[Instant]](Some(Instant.MIN))((acc, id) => {
          val let = acs.activeContracts
            .getOrElse(
              id,
              throw MissingContracts(Set(id)),
            )
            .let
          acc.map(acc => if (let.isAfter(acc)) let else acc)
        })
        .map(Timestamp.assertFromInstant)
    }))

  override def publishTransaction(
      submitterInfo: state.SubmitterInfo,
      transactionMeta: state.TransactionMeta,
      transaction: SubmittedTransaction,
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Future.successful(
      this.synchronized[state.SubmissionResult] {
        handleSuccessfulTx(entries.nextTransactionId, submitterInfo, transactionMeta, transaction)
        state.SubmissionResult.Acknowledged
      }
    )

  // Validates the given ledger time according to the ledger time model
  private def checkTimeModel(
      ledgerTime: Timestamp,
      recordTime: Timestamp,
  ): Either[Rejection, Unit] =
    ledgerConfiguration
      .toRight(Rejection.NoLedgerConfiguration)
      .flatMap(config =>
        config.timeModel.checkTime(ledgerTime, recordTime).left.map(Rejection.InvalidLedgerTime)
      )

  private def handleSuccessfulTx(
      transactionId: Ref.LedgerString,
      submitterInfo: state.SubmitterInfo,
      transactionMeta: state.TransactionMeta,
      transaction: SubmittedTransaction,
  )(implicit loggingContext: LoggingContext): Unit = {
    val ledgerTime = transactionMeta.ledgerEffectiveTime
    val recordTime = timeProvider.getCurrentTimestamp
    checkTimeModel(ledgerTime, recordTime)
      .fold(
        rejection => handleError(submitterInfo, rejection.toDomainRejectionReason),
        _ => {
          val (committedTransaction, disclosureForIndex, divulgence) =
            Ledger
              .convertToCommittedTransaction(
                transactionCommitter,
                transactionId,
                transaction,
              )
          val acsRes = acs.addTransaction(
            transactionMeta.ledgerEffectiveTime.toInstant,
            transactionId,
            transactionMeta.workflowId,
            submitterInfo.actAs,
            committedTransaction,
            disclosureForIndex,
            divulgence,
            List.empty,
          )
          acsRes match {
            case Left(err) =>
              handleError(
                submitterInfo,
                RejectionReason.Inconsistent(s"Reason: ${err.mkString("[", ", ", "]")}"),
              )
            case Right(newAcs) =>
              acs = newAcs
              val entry = LedgerEntry
                .Transaction(
                  Some(submitterInfo.commandId),
                  transactionId,
                  Some(submitterInfo.applicationId),
                  submitterInfo.submissionId,
                  submitterInfo.actAs,
                  transactionMeta.workflowId,
                  transactionMeta.ledgerEffectiveTime,
                  recordTime,
                  committedTransaction,
                  disclosureForIndex,
                )
              entries.publish(InMemoryLedgerEntry(entry))
              ()
          }
        },
      )

  }

  private def handleError(submitterInfo: state.SubmitterInfo, reason: RejectionReason)(implicit
      loggingContext: LoggingContext
  ): Unit = {
    logger.warn(s"Publishing error to ledger: ${reason.description}")
    stopDeduplicatingCommand(CommandId(submitterInfo.commandId), submitterInfo.actAs)
    entries.publish(
      InMemoryLedgerEntry(
        LedgerEntry.Rejection(
          timeProvider.getCurrentTimestamp,
          submitterInfo.commandId,
          submitterInfo.applicationId,
          submitterInfo.submissionId,
          submitterInfo.actAs,
          reason,
        )
      )
    )
    ()
  }

  override def close(): Unit = ()

  private def filterFor(requestingParties: Set[Ref.Party]): TransactionFilter =
    TransactionFilter(requestingParties.map(p => p -> Filters.noFilter).toMap)

  private def lookupTransactionEntry(
      id: Ref.TransactionId
  ): Option[(Offset, LedgerEntry.Transaction)] =
    entries.items
      .collectFirst {
        case (offset, InMemoryLedgerEntry(tx: LedgerEntry.Transaction)) if tx.transactionId == id =>
          (offset, enrichTX(tx))
      }

  override def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    Future.successful {
      lookupTransactionEntry(transactionId).flatMap { case (offset, entry) =>
        TransactionConversion
          .ledgerEntryToFlatTransaction(
            offset = LedgerOffset.Absolute(ApiOffset.toApiString(offset)),
            entry = entry,
            filter = filterFor(requestingParties),
            verbose = true,
          )
          .map(tx => GetFlatTransactionResponse(Some(tx)))
      }
    }

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    Future.successful {
      lookupTransactionEntry(transactionId).flatMap { case (offset, entry) =>
        TransactionConversion
          .ledgerEntryToTransactionTree(
            offset = LedgerOffset.Absolute(ApiOffset.toApiString(offset)),
            entry = entry,
            requestingParties = requestingParties,
            verbose = true,
          )
          .map(tx => GetTransactionResponse(Some(tx)))
      }
    }

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    Future.successful(this.synchronized {
      parties.flatMap(party => acs.parties.get(party).toList).toList
    })

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    Future.successful(this.synchronized {
      acs.parties.values.toList
    })

  override def publishPartyAllocation(
      submissionId: Ref.SubmissionId,
      party: Ref.Party,
      displayName: Option[String],
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Future.successful(this.synchronized[state.SubmissionResult] {
      val ids = acs.parties.keySet
      if (ids.contains(party)) {
        entries.publish(
          InMemoryPartyEntry(
            PartyLedgerEntry.AllocationRejected(
              submissionId,
              timeProvider.getCurrentTimestamp,
              "Party already exists",
            )
          )
        )
      } else {
        acs = acs.addParty(PartyDetails(party, displayName, isLocal = true))
        entries.publish(
          InMemoryPartyEntry(
            PartyLedgerEntry.AllocationAccepted(
              Some(submissionId),
              timeProvider.getCurrentTimestamp,
              PartyDetails(party, displayName, isLocal = true),
            )
          )
        )
      }
      state.SubmissionResult.Acknowledged
    })

  override def partyEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    entries.getSource(Some(startExclusive), None).collect {
      case (offset, InMemoryPartyEntry(partyEntry)) => (offset, partyEntry)
    }
  }

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, PackageDetails]] =
    packageStoreRef.get.listLfPackages()

  override def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Archive]] =
    packageStoreRef.get.getLfArchive(packageId)

  override def getLfPackage(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Ast.Package]] =
    packageStoreRef.get.getLfPackage(packageId)

  override def packageEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PackageLedgerEntry), NotUsed] =
    entries.getSource(Some(startExclusive), None).collect {
      case (offset, InMemoryPackageEntry(entry)) => (offset, entry)
    }

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      knownSince: Timestamp,
      sourceDescription: Option[String],
      payload: List[Archive],
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] = {

    val oldStore = packageStoreRef.get
    oldStore
      .withPackages(knownSince, sourceDescription, payload)
      .fold(
        err => {
          entries.publish(
            InMemoryPackageEntry(
              PackageLedgerEntry
                .PackageUploadRejected(submissionId, timeProvider.getCurrentTimestamp, err)
            )
          )
          Future.successful(state.SubmissionResult.Acknowledged)
        },
        newStore => {
          if (packageStoreRef.compareAndSet(oldStore, newStore)) {
            entries.publish(
              InMemoryPackageEntry(
                PackageLedgerEntry
                  .PackageUploadAccepted(submissionId, timeProvider.getCurrentTimestamp)
              )
            )
            Future.successful(state.SubmissionResult.Acknowledged)
          } else {
            uploadPackages(submissionId, knownSince, sourceDescription, payload)
          }
        },
      )
  }

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration,
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Future.successful {
      this.synchronized {
        val recordTime = timeProvider.getCurrentTimestamp
        ledgerConfiguration match {
          case Some(currentConfig) if config.generation != currentConfig.generation + 1 =>
            entries.publish(
              InMemoryConfigEntry(
                ConfigurationEntry.Rejected(
                  submissionId,
                  s"Generation mismatch, expected ${currentConfig.generation + 1}, got ${config.generation}",
                  config,
                )
              )
            )

          case _ if recordTime > maxRecordTime =>
            entries.publish(
              InMemoryConfigEntry(
                ConfigurationEntry.Rejected(
                  submissionId,
                  s"Configuration change timed out: $maxRecordTime > $recordTime",
                  config,
                )
              )
            )
            ledgerConfiguration = Some(config)

          case _ =>
            entries.publish(InMemoryConfigEntry(ConfigurationEntry.Accepted(submissionId, config)))
            ledgerConfiguration = Some(config)
        }
        state.SubmissionResult.Acknowledged
      }
    }

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    Future.successful(this.synchronized {
      val end = ledgerEnd()
      ledgerConfiguration.map(config => end -> config)
    })

  override def configurationEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, ConfigurationEntry), NotUsed] =
    entries
      .getSource(Some(startExclusive), None)
      .collect { case (offset, InMemoryConfigEntry(entry)) =>
        offset -> entry
      }

  private def deduplicationKey(
      commandId: CommandId,
      submitters: List[Ref.Party],
  ): String = {
    val submitterPart =
      if (submitters.length == 1)
        submitters.head
      else
        submitters.sorted(Ordering.String).distinct.mkString("%")
    commandId.unwrap + "%" + submitterPart
  }

  override def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    Future.successful {
      this.synchronized {
        val key = deduplicationKey(commandId, submitters)
        val entry = commands.get(key)
        if (entry.isEmpty) {
          // No previous entry - new command
          commands += (key -> CommandDeduplicationEntry(key, deduplicateUntil.toInstant))
          CommandDeduplicationNew
        } else {
          val previousDeduplicateUntil = entry.get.deduplicateUntil
          if (submittedAt.toInstant.isAfter(previousDeduplicateUntil)) {
            // Previous entry expired - new command
            commands += (key -> CommandDeduplicationEntry(key, deduplicateUntil.toInstant))
            CommandDeduplicationNew
          } else {
            // Existing previous entry - deduplicate command
            CommandDeduplicationDuplicate(Timestamp.assertFromInstant(previousDeduplicateUntil))
          }
        }
      }
    }

  override def removeExpiredDeduplicationData(currentTime: Timestamp)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Future.successful {
      this.synchronized {
        commands.retain((_, v) => v.deduplicateUntil.isAfter(currentTime.toInstant))
        ()
      }
    }

  override def stopDeduplicatingCommand(commandId: CommandId, submitters: List[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Future.successful {
      val key = deduplicationKey(commandId, submitters)
      this.synchronized {
        commands.remove(key)
        ()
      }
    }

  override def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    // sandbox-classic in-memory ledger does not support pruning
    Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
}

private[sandbox] object InMemoryLedger {

  sealed trait InMemoryEntry extends Product with Serializable

  final case class InMemoryLedgerEntry(entry: LedgerEntry) extends InMemoryEntry

  final case class InMemoryConfigEntry(entry: ConfigurationEntry) extends InMemoryEntry

  final case class InMemoryPartyEntry(entry: PartyLedgerEntry) extends InMemoryEntry

  final case class InMemoryPackageEntry(entry: PackageLedgerEntry) extends InMemoryEntry

  final case class CommandDeduplicationEntry(
      deduplicationKey: String,
      deduplicateUntil: Instant,
  )

}
