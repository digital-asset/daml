// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.inmemory

import java.time.Instant
import java.util.UUID
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
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  Filters,
  LedgerId,
  LedgerOffset,
  PartyDetails,
  RejectionReason,
  TransactionFilter
}
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse
}
import com.digitalasset.platform.index.TransactionConversion
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.digitalasset.platform.store.{CompletionFromTransaction, LedgerSnapshot}
import com.digitalasset.platform.{ApiOffset, index}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future

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
    packageStoreInit: InMemoryPackageStore,
    ledgerEntries: ImmArray[LedgerEntryOrBump],
    initialConfig: Configuration,
) extends Ledger {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val entries = {
    val l = new LedgerEntries[InMemoryEntry](_.toString)
    l.publish(
      InMemoryConfigEntry(
        ConfigurationEntry.Accepted(
          submissionId = UUID.randomUUID.toString,
          participantId = participantId,
          configuration = initialConfig,
        )))
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

  override def ledgerEntries(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset]): Source[(Offset, LedgerEntry), NotUsed] =
    entries
      .getSource(startExclusive, endInclusive)
      .collect {
        case (offset, InMemoryLedgerEntry(entry)) => offset -> entry
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

  // need to take the lock to make sure the two pieces of data are consistent.
  override def snapshot(filter: TransactionFilter): Future[LedgerSnapshot] =
    Future.successful(this.synchronized {
      LedgerSnapshot(
        entries.ledgerEnd,
        Source
          .fromIterator[ActiveContract](() =>
            acs.activeContracts.valuesIterator.flatMap(index.EventFilter(_)(filter).toList))
      )
    })

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

  override def lookupMaximumLedgerTime(contractIds: Set[AbsoluteContractId]): Future[Instant] =
    Future.successful(this.synchronized {
      contractIds.foldLeft[Instant](Instant.EPOCH)((acc, id) => {
        val let = acs.activeContracts
          .getOrElse(id, sys.error(s"Contract $id not found while looking for maximum ledger time"))
          .let
        if (let.isAfter(acc)) let else acc
      })
    })

  override def publishHeartbeat(time: Instant): Future[Unit] =
    Future.successful(this.synchronized[Unit] {
      entries.publish(InMemoryLedgerEntry(LedgerEntry.Checkpoint(time)))
      ()
    })

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

  private def handleSuccessfulTx(
      transactionId: LedgerString,
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Unit = {
    val recordTime = timeProvider.getCurrentTime
    if (recordTime.isAfter(submitterInfo.maxRecordTime.toInstant)) {
      // This can happen if the DAML-LF computation (i.e. exercise of a choice) takes longer
      // than the time window between LET and MRT allows for.
      // See https://github.com/digital-asset/daml/issues/987
      handleError(
        submitterInfo,
        RejectionReason.TimedOut(
          s"RecordTime $recordTime is after MaxiumRecordTime ${submitterInfo.maxRecordTime}"))
    } else {
      val (transactionForIndex, disclosureForIndex, globalDivulgence) =
        Ledger.convertToCommittedTransaction(transactionId, transaction)
      // 5b. modify the ActiveContracts, while checking that we do not have double
      // spends or timing issues
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

  }

  private def handleError(submitterInfo: SubmitterInfo, reason: RejectionReason): Unit = {
    logger.warn(s"Publishing error to ledger: ${reason.description}")
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

  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    Future.successful {
      this.synchronized {
        val entry = commands.get(deduplicationKey)
        if (entry.isEmpty) {
          // No previous entry - new command
          commands += (deduplicationKey -> CommandDeduplicationEntry(
            deduplicationKey,
            deduplicateUntil))
          CommandDeduplicationNew
        } else {
          val previousDeduplicateUntil = entry.get.deduplicateUntil
          if (submittedAt.isAfter(previousDeduplicateUntil)) {
            // Previous entry expired - new command
            commands += (deduplicationKey -> CommandDeduplicationEntry(
              deduplicationKey,
              deduplicateUntil))
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
}
