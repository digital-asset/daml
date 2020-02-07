// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.inmemory

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{
  ApplicationId => _,
  LedgerId => _,
  TransactionId => _,
  _
}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  CommandId,
  LedgerId,
  PartyDetails,
  RejectionReason,
  TransactionId
}
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import com.digitalasset.platform.sandbox.stores.deduplicator.Deduplicator
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.digitalasset.platform.store.{Contract, LedgerSnapshot}
import org.slf4j.LoggerFactory
import scalaz.Tag

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

sealed trait InMemoryEntry extends Product with Serializable
final case class InMemoryLedgerEntry(entry: LedgerEntry) extends InMemoryEntry
final case class InMemoryConfigEntry(entry: ConfigurationEntry) extends InMemoryEntry
final case class InMemoryPartyEntry(entry: PartyLedgerEntry) extends InMemoryEntry
final case class InMemoryPackageEntry(entry: PackageLedgerEntry) extends InMemoryEntry

/** This stores all the mutable data that we need to run a ledger: the PCS, the ACS, and the deduplicator.
  *
  */
class InMemoryLedger(
    val ledgerId: LedgerId,
    participantId: ParticipantId,
    timeProvider: TimeProvider,
    acs0: InMemoryActiveLedgerState,
    packageStoreInit: InMemoryPackageStore,
    ledgerEntries: ImmArray[LedgerEntryOrBump])
    extends Ledger {

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

  override def ledgerEntries(
      beginInclusive: Option[Long],
      endExclusive: Option[Long]): Source[(Long, LedgerEntry), NotUsed] =
    entries
      .getSource(beginInclusive, endExclusive)
      .collect { case (offset, InMemoryLedgerEntry(entry)) => offset -> entry }

  // mutable state
  private var acs = acs0
  private var deduplicator = Deduplicator()
  private var ledgerConfiguration: Option[Configuration] = None

  override def ledgerEnd: Long = entries.ledgerEnd

  // need to take the lock to make sure the two pieces of data are consistent.
  override def snapshot(filter: TemplateAwareFilter): Future[LedgerSnapshot] =
    Future.successful(this.synchronized {
      LedgerSnapshot(
        entries.ledgerEnd,
        Source.fromIterator[ActiveContract](() => acs.activeContracts.valuesIterator))
    })

  override def lookupContract(
      contractId: AbsoluteContractId,
      forParty: Party
  ): Future[Option[Contract]] =
    Future.successful(this.synchronized {
      acs.activeContracts.get(contractId).filter(ac => acs.isVisibleForDivulgees(ac.id, forParty))
    })

  override def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    Future.successful(this.synchronized {
      acs.keys.get(key).filter(acs.isVisibleForStakeholders(_, forParty))
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
        val (newDeduplicator, isDuplicate) =
          deduplicator.checkAndAdd(
            submitterInfo.submitter,
            ApplicationId(submitterInfo.applicationId),
            CommandId(submitterInfo.commandId))
        deduplicator = newDeduplicator
        if (isDuplicate)
          logger.warn(
            "Ignoring duplicate submission for applicationId {}, commandId {}",
            submitterInfo.applicationId: Any,
            submitterInfo.commandId)
        else
          handleSuccessfulTx(entries.toLedgerString, submitterInfo, transactionMeta, transaction)

        SubmissionResult.Acknowledged
      }
    )

  private def handleSuccessfulTx(
      trId: LedgerString,
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

      val blindingInfo = Blinding.blind(transaction)
      val mappedDisclosure = blindingInfo.disclosure.map {
        case (nodeId, v) => EventIdFormatter.fromTransactionId(trId, nodeId) -> v
      }
      val mappedLocalDivulgence = blindingInfo.localDivulgence.map {
        case (nodeId, v) => EventIdFormatter.fromTransactionId(trId, nodeId) -> v
      }
      val mappedGlobalDivulgence = blindingInfo.globalDivulgence

      val mappedTx =
        transaction
          .resolveRelCid(EventIdFormatter.makeAbs(trId))
          .mapNodeId(EventIdFormatter.fromTransactionId(trId, _))
      // 5b. modify the ActiveContracts, while checking that we do not have double
      // spends or timing issues
      val acsRes = acs.addTransaction(
        transactionMeta.ledgerEffectiveTime.toInstant,
        trId,
        transactionMeta.workflowId,
        Some(submitterInfo.submitter),
        mappedTx,
        mappedDisclosure,
        mappedLocalDivulgence,
        mappedGlobalDivulgence,
        List.empty
      )
      acsRes match {
        case Left(err) =>
          handleError(
            submitterInfo,
            RejectionReason.Inconsistent(s"Reason: ${err.mkString("[", ", ", "]")}"))
        case Right(newAcs) =>
          acs = newAcs
          val recordBlinding =
            blindingInfo.disclosure.map {
              case (nid, parties) =>
                (EventIdFormatter.fromTransactionId(trId, nid), parties)
            }
          val entry = LedgerEntry
            .Transaction(
              Some(submitterInfo.commandId),
              trId,
              Some(submitterInfo.applicationId),
              Some(submitterInfo.submitter),
              transactionMeta.workflowId,
              transactionMeta.ledgerEffectiveTime.toInstant,
              recordTime,
              mappedTx,
              recordBlinding
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

  override def lookupTransaction(
      transactionId: TransactionId): Future[Option[(Long, LedgerEntry.Transaction)]] = {

    Try(Tag.unwrap(transactionId).toLong) match {
      case Failure(_) =>
        Future.successful(None)
      case Success(n) =>
        Future.successful(
          entries
            .getEntryAt(n)
            .collect {
              case InMemoryLedgerEntry(t: LedgerEntry.Transaction) =>
                (n, t) // the transaction id is also the offset
            })
    }
  }

  override def parties: Future[List[PartyDetails]] =
    Future.successful(this.synchronized {
      acs.parties.values.toList
    })

  override def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]): Future[SubmissionResult] =
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

  override def partyEntries(beginOffset: Long): Source[(Long, PartyLedgerEntry), NotUsed] = {
    entries.getSource(Some(beginOffset), None).collect {
      case (offset, InMemoryPartyEntry(partyEntry)) => (offset, partyEntry)
    }
  }

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    packageStoreRef.get.listLfPackages()

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    packageStoreRef.get.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    packageStoreRef.get.getLfPackage(packageId)

  override def packageEntries(beginOffset: Long): Source[(Long, PackageLedgerEntry), NotUsed] =
    entries.getSource(Some(beginOffset), None).collect {
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

  override def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]] =
    Future.successful(this.synchronized {
      ledgerConfiguration.map(config => ledgerEnd -> config)
    })

  override def configurationEntries(
      startInclusive: Option[Long]): Source[(Long, ConfigurationEntry), NotUsed] =
    entries
      .getSource(startInclusive, None)
      .collect { case (offset, InMemoryConfigEntry(entry)) => offset -> entry }

}
