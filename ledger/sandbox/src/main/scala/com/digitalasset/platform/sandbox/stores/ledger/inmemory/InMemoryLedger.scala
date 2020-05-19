// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.inmemory

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  PackageDetails
}
import com.daml.ledger.participant.state.v1.{
  ApplicationId => _,
  LedgerId => _,
  TransactionId => _,
  _
}
import com.daml.api.util.TimeProvider
import com.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{Node, TransactionCommitter}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger
import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  Filters,
  InclusiveFilters,
  LedgerId,
  LedgerOffset,
  PartyDetails,
  RejectionReason,
  TransactionFilter
}
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.platform.index.TransactionConversion
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.store.Contract.ActiveContract
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.{ApiOffset, index}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future
import scala.util.Try

sealed trait InMemoryEntry extends Product with Serializable
final case class InMemoryLedgerEntry(entry: LedgerEntry) extends InMemoryEntry
final case class InMemoryConfigEntry(entry: ConfigurationEntry) extends InMemoryEntry
final case class InMemoryPartyEntry(entry: PartyLedgerEntry) extends InMemoryEntry
final case class InMemoryPackageEntry(entry: PackageLedgerEntry) extends InMemoryEntry

final case class CommandDeduplicationEntry(
    deduplicationKey: String,
    deduplicateUntil: Instant,
)

/** This stores all the mutable data that we need to run a ledger: the PCS, the ACS, and the deduplicator.
  *
  */
class InMemoryLedger(
    val ledgerId: LedgerId,
    participantId: ParticipantId,
    timeProvider: TimeProvider,
    acs0: InMemoryActiveLedgerState,
    transactionCommitter: TransactionCommitter,
    packageStoreInit: InMemoryPackageStore,
    ledgerEntries: ImmArray[LedgerEntryOrBump],
) extends Ledger {

  private val logger = LoggerFactory.getLogger(this.getClass)

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

  def ledgerEntries(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset]): Source[(Offset, LedgerEntry), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .collect {
        case (offset, InMemoryLedgerEntry(entry)) => offset -> entry
      }

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Party, Set[Ref.Identifier]],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .flatMapConcat {
        case (offset, InMemoryLedgerEntry(tx: LedgerEntry.Transaction)) =>
          Source(
            TransactionConversion
              .ledgerEntryToFlatTransaction(
                LedgerOffset.Absolute(ApiOffset.toApiString(offset)),
                tx,
                TransactionFilter(filter.map {
                  case (party, templates) =>
                    party -> Filters(
                      if (templates.nonEmpty) Some(InclusiveFilters(templates)) else None)
                }),
                verbose
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
      requestingParties: Set[Party],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .flatMapConcat {
        case (offset, InMemoryLedgerEntry(tx: LedgerEntry.Transaction)) =>
          Source(
            TransactionConversion
              .ledgerEntryToTransactionTree(
                LedgerOffset.Absolute(ApiOffset.toApiString(offset)),
                tx,
                requestingParties,
                verbose)
              .map(tx => offset -> GetTransactionTreesResponse(Seq(tx)))
              .toList)
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
      parties: Set[Party]): Source[(Offset, CompletionStreamResponse), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .collect {
        case (offset, InMemoryLedgerEntry(entry)) => (offset, entry)
      }
      .collect(CompletionFromTransaction(applicationId.unwrap, parties))

  override def ledgerEnd: Offset = entries.ledgerEnd

  override def activeContracts(
      filter: Map[Party, Set[Ref.Identifier]],
      verbose: Boolean,
  ): (Source[GetActiveContractsResponse, NotUsed], Offset) = {
    val (acsNow, ledgerEndNow) = this.synchronized { (acs, ledgerEnd) }
    (
      Source
        .fromIterator[ActiveContract](
          () =>
            acsNow.activeContracts.valuesIterator.flatMap(
              index
                .EventFilter(_)(TransactionFilter(filter.map {
                  case (party, templates) =>
                    party -> Filters(
                      if (templates.nonEmpty) Some(InclusiveFilters(templates)) else None)
                }))
                .toList))
        .map { contract =>
          GetActiveContractsResponse(
            workflowId = contract.workflowId.getOrElse(""),
            activeContracts = List(
              CreatedEvent(
                contract.eventId,
                contract.id.coid,
                Some(LfEngineToApi.toApiIdentifier(contract.contract.template)),
                contractKey = contract.key.map(
                  ck =>
                    LfEngineToApi.assertOrRuntimeEx(
                      "converting stored contract",
                      LfEngineToApi
                        .lfContractKeyToApiValue(verbose = verbose, ck))),
                createArguments = Some(
                  LfEngineToApi.assertOrRuntimeEx(
                    "converting stored contract",
                    LfEngineToApi
                      .lfValueToApiRecord(verbose = verbose, contract.contract.arg.value))),
                contract.signatories.union(contract.observers).intersect(filter.keySet).toSeq,
                signatories = contract.signatories.toSeq,
                observers = contract.observers.toSeq,
                agreementText = Some(contract.agreementText)
              )
            )
          )
        },
      ledgerEndNow)
  }

  override def lookupContract(
      contractId: AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    Future.successful(this.synchronized {
      acs.activeContracts
        .get(contractId)
        .filter(ac => acs.isVisibleForDivulgees(ac.id, forParty))
        .map(_.contract)
    })

  override def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    Future.successful(this.synchronized {
      acs.keys.get(key).filter(acs.isVisibleForStakeholders(_, forParty))
    })

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId]): Future[Option[Instant]] =
    if (contractIds.isEmpty) {
      Future.failed(
        new IllegalArgumentException(
          "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
        )
      )
    } else {
      Future.fromTry(Try(this.synchronized {
        contractIds.foldLeft[Option[Instant]](Some(Instant.MIN))((acc, id) => {
          val let = acs.activeContracts
            .getOrElse(
              id,
              sys.error(s"Contract $id not found while looking for maximum ledger time"))
            .let
          acc.map(acc => if (let.isAfter(acc)) let else acc)
        })
      }))
    }

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult] =
    Future.successful(
      this.synchronized[SubmissionResult] {
        handleSuccessfulTx(entries.nextTransactionId, submitterInfo, transactionMeta, transaction)
        SubmissionResult.Acknowledged
      }
    )

  // Validates the given ledger time according to the ledger time model
  private def checkTimeModel(ledgerTime: Instant, recordTime: Instant): Either[String, Unit] = {
    ledgerConfiguration
      .fold[Either[String, Unit]](
        Left("No ledger configuration available, cannot validate ledger time")
      )(
        config => config.timeModel.checkTime(ledgerTime, recordTime)
      )
  }

  private def handleSuccessfulTx(
      transactionId: LedgerString,
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Unit = {
    val ledgerTime = transactionMeta.ledgerEffectiveTime.toInstant
    val recordTime = timeProvider.getCurrentTime
    checkTimeModel(ledgerTime, recordTime)
      .fold(
        reason => handleError(submitterInfo, RejectionReason.InvalidLedgerTime(reason)),
        _ => {
          val (transactionForIndex, disclosureForIndex, globalDivulgence) =
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
            Some(submitterInfo.submitter),
            transactionForIndex,
            disclosureForIndex,
            globalDivulgence,
            List.empty
          )
          acsRes match {
            case Left(err) =>
              handleError(
                submitterInfo,
                RejectionReason.Inconsistent(s"Reason: ${err.mkString("[", ", ", "]")}"))
            case Right(newAcs) =>
              acs = newAcs
              val entry = LedgerEntry
                .Transaction(
                  Some(submitterInfo.commandId),
                  transactionId,
                  Some(submitterInfo.applicationId),
                  Some(submitterInfo.submitter),
                  transactionMeta.workflowId,
                  transactionMeta.ledgerEffectiveTime.toInstant,
                  recordTime,
                  transactionForIndex,
                  disclosureForIndex
                )
              entries.publish(InMemoryLedgerEntry(entry))
              ()
          }
        }
      )

  }

  private def handleError(submitterInfo: SubmitterInfo, reason: RejectionReason): Unit = {
    logger.warn(s"Publishing error to ledger: ${reason.description}")
    stopDeduplicatingCommand(CommandId(submitterInfo.commandId), submitterInfo.submitter)
    entries.publish(
      InMemoryLedgerEntry(
        LedgerEntry.Rejection(
          timeProvider.getCurrentTime,
          submitterInfo.commandId,
          submitterInfo.applicationId,
          submitterInfo.submitter,
          reason)
      )
    )
    ()
  }

  override def close(): Unit = ()

  private def filterFor(requestingParties: Set[Party]): TransactionFilter =
    TransactionFilter(requestingParties.map(p => p -> Filters.noFilter).toMap)

  private def lookupTransactionEntry(
      id: ledger.TransactionId,
  ): Option[(Offset, LedgerEntry.Transaction)] =
    entries.items
      .collectFirst {
        case (offset, InMemoryLedgerEntry(tx: LedgerEntry.Transaction)) if tx.transactionId == id =>
          (offset, tx)
      }

  override def lookupFlatTransactionById(
      transactionId: ledger.TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]] =
    Future.successful {
      lookupTransactionEntry(transactionId).flatMap {
        case (offset, entry) =>
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
      transactionId: ledger.TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]] =
    Future.successful {
      lookupTransactionEntry(transactionId).flatMap {
        case (offset, entry) =>
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

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    Future.successful(this.synchronized {
      parties.flatMap(party => acs.parties.get(party).toList).toList
    })

  override def listKnownParties(): Future[List[PartyDetails]] =
    Future.successful(this.synchronized {
      acs.parties.values.toList
    })

  override def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]
  ): Future[SubmissionResult] =
    Future.successful(this.synchronized[SubmissionResult] {
      val ids = acs.parties.keySet
      if (ids.contains(party)) {
        entries.publish(
          InMemoryPartyEntry(
            PartyLedgerEntry.AllocationRejected(
              submissionId,
              participantId,
              timeProvider.getCurrentTime,
              "Party already exists")))
      } else {
        acs = acs.addParty(PartyDetails(party, displayName, isLocal = true))
        entries.publish(
          InMemoryPartyEntry(
            PartyLedgerEntry.AllocationAccepted(
              Some(submissionId),
              participantId,
              timeProvider.getCurrentTime,
              PartyDetails(party, displayName, isLocal = true))))
      }
      SubmissionResult.Acknowledged
    })

  override def partyEntries(startExclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    entries.getSource(Some(startExclusive), None).collect {
      case (offset, InMemoryPartyEntry(partyEntry)) => (offset, partyEntry)
    }
  }

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    packageStoreRef.get.listLfPackages()

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    packageStoreRef.get.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    packageStoreRef.get.getLfPackage(packageId)

  override def packageEntries(
      startExclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    entries.getSource(Some(startExclusive), None).collect {
      case (offset, InMemoryPackageEntry(entry)) => (offset, entry)
    }

  override def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[SubmissionResult] = {

    val oldStore = packageStoreRef.get
    oldStore
      .withPackages(knownSince, sourceDescription, payload)
      .fold(
        err => {
          entries.publish(
            InMemoryPackageEntry(PackageLedgerEntry
              .PackageUploadRejected(submissionId, timeProvider.getCurrentTime, err)))
          Future.successful(SubmissionResult.Acknowledged)
        },
        newStore => {
          if (packageStoreRef.compareAndSet(oldStore, newStore)) {
            entries.publish(InMemoryPackageEntry(
              PackageLedgerEntry.PackageUploadAccepted(submissionId, timeProvider.getCurrentTime)))
            Future.successful(SubmissionResult.Acknowledged)
          } else {
            uploadPackages(submissionId, knownSince, sourceDescription, payload)
          }
        }
      )
  }

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): Future[SubmissionResult] =
    Future.successful {
      this.synchronized {
        val recordTime = timeProvider.getCurrentTime
        val mrt = maxRecordTime.toInstant
        ledgerConfiguration match {
          case Some(currentConfig) if config.generation != currentConfig.generation + 1 =>
            entries.publish(
              InMemoryConfigEntry(
                ConfigurationEntry.Rejected(
                  submissionId,
                  participantId,
                  s"Generation mismatch, expected ${currentConfig.generation + 1}, got ${config.generation}",
                  config)))

          case _ if recordTime.isAfter(mrt) =>
            entries.publish(
              InMemoryConfigEntry(
                ConfigurationEntry.Rejected(
                  submissionId,
                  participantId,
                  s"Configuration change timed out: $mrt > $recordTime",
                  config)))
            ledgerConfiguration = Some(config)

          case _ =>
            entries.publish(
              InMemoryConfigEntry(ConfigurationEntry.Accepted(submissionId, participantId, config)))
            ledgerConfiguration = Some(config)
        }
        SubmissionResult.Acknowledged
      }
    }

  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    Future.successful(this.synchronized {
      ledgerConfiguration.map(config => ledgerEnd -> config)
    })

  override def configurationEntries(
      startExclusive: Option[Offset]): Source[(Offset, ConfigurationEntry), NotUsed] =
    entries
      .getSource(startExclusive, None)
      .collect {
        case (offset, InMemoryConfigEntry(entry)) => offset -> entry
      }

  private def deduplicationKey(
      commandId: CommandId,
      submitter: Ref.Party,
  ): String = commandId.unwrap + "%" + submitter

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    Future.successful {
      this.synchronized {
        val key = deduplicationKey(commandId, submitter)
        val entry = commands.get(key)
        if (entry.isEmpty) {
          // No previous entry - new command
          commands += (key -> CommandDeduplicationEntry(key, deduplicateUntil))
          CommandDeduplicationNew
        } else {
          val previousDeduplicateUntil = entry.get.deduplicateUntil
          if (submittedAt.isAfter(previousDeduplicateUntil)) {
            // Previous entry expired - new command
            commands += (key -> CommandDeduplicationEntry(key, deduplicateUntil))
            CommandDeduplicationNew
          } else {
            // Existing previous entry - deduplicate command
            CommandDeduplicationDuplicate(previousDeduplicateUntil)
          }
        }
      }
    }

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    Future.successful {
      this.synchronized {
        commands.retain((_, v) => v.deduplicateUntil.isAfter(currentTime))
        ()
      }
    }

  override def stopDeduplicatingCommand(commandId: CommandId, submitter: Party): Future[Unit] =
    Future.successful {
      val key = deduplicationKey(commandId, submitter)
      this.synchronized {
        commands.remove(key)
        ()
      }
    }
}
