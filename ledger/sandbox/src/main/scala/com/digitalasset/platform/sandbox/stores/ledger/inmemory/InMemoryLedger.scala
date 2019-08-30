// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.inmemory

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{
  PartyAllocationResult,
  SubmissionResult,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta,
  UploadPackagesResult
}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, TransactionIdString}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  CommandId,
  LedgerId,
  PartyDetails,
  RejectionReason
}
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.ActiveContract
import com.digitalasset.platform.sandbox.stores.deduplicator.Deduplicator
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.{Checkpoint, Rejection}
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, LedgerEntry, LedgerSnapshot}
import com.digitalasset.platform.sandbox.stores.{
  ActiveLedgerState,
  InMemoryActiveLedgerState,
  InMemoryPackageStore
}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** This stores all the mutable data that we need to run a ledger: the PCS, the ACS, and the deduplicator.
  *
  */
class InMemoryLedger(
    val ledgerId: LedgerId,
    timeProvider: TimeProvider,
    acs0: InMemoryActiveLedgerState,
    packageStoreInit: InMemoryPackageStore,
    ledgerEntries: ImmArray[LedgerEntryOrBump])
    extends Ledger {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val entries = {
    val l = new LedgerEntries[LedgerEntry](_.toString)
    ledgerEntries.foreach {
      case LedgerEntryOrBump.Bump(increment) =>
        l.incrementOffset(increment)
        ()
      case LedgerEntryOrBump.Entry(entry) =>
        l.publish(entry)
        ()
    }
    l
  }

  private val packageStoreRef = new AtomicReference[InMemoryPackageStore](packageStoreInit)

  override def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed] =
    entries.getSource(offset)

  // mutable state
  private var acs = acs0
  private var deduplicator = Deduplicator()

  override def ledgerEnd: Long = entries.ledgerEnd

  // need to take the lock to make sure the two pieces of data are consistent.
  override def snapshot(): Future[LedgerSnapshot] =
    Future.successful(this.synchronized {
      LedgerSnapshot(entries.ledgerEnd, Source[ActiveContract](acs.contracts.map(_._2)))
    })

  override def lookupContract(
      contractId: AbsoluteContractId): Future[Option[ActiveLedgerState.ActiveContract]] =
    Future.successful(this.synchronized {
      acs.contracts.get(contractId)
    })

  override def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]] =
    Future.successful(this.synchronized {
      acs.keys.get(key)
    })

  override def publishHeartbeat(time: Instant): Future[Unit] =
    Future.successful(this.synchronized[Unit] {
      entries.publish(Checkpoint(time))
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
          handleSuccessfulTx(entries.toTransactionId, submitterInfo, transactionMeta, transaction)

        SubmissionResult.Acknowledged
      }
    )

  private def handleSuccessfulTx(
      trId: TransactionIdString,
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
      val toAbsCoid: ContractId => AbsoluteContractId =
        SandboxEventIdFormatter.makeAbsCoid(trId)

      val blindingInfo = Blinding.blind(transaction)

      val mappedTx = transaction.mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
      // 5b. modify the ActiveContracts, while checking that we do not have double
      // spends or timing issues
      val acsRes = acs.addTransaction(
        transactionMeta.ledgerEffectiveTime.toInstant,
        trId,
        transactionMeta.workflowId,
        mappedTx,
        blindingInfo.explicitDisclosure,
        blindingInfo.localImplicitDisclosure,
        blindingInfo.globalImplicitDisclosure,
      )
      acsRes match {
        case Left(err) =>
          handleError(
            submitterInfo,
            RejectionReason.Inconsistent(s"Reason: ${err.mkString("[", ", ", "]")}"))
        case Right(newAcs) =>
          acs = newAcs
          val recordTx = mappedTx
            .mapNodeId(SandboxEventIdFormatter.fromTransactionId(trId, _))
          val recordBlinding =
            blindingInfo.explicitDisclosure.map {
              case (nid, parties) =>
                (SandboxEventIdFormatter.fromTransactionId(trId, nid), parties)
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
              recordTx,
              recordBlinding
            )
          entries.publish(entry)
          ()
      }
    }

  }

  private def handleError(submitterInfo: SubmitterInfo, reason: RejectionReason): Unit = {
    logger.warn(s"Publishing error to ledger: ${reason.description}")
    entries.publish(
      Rejection(
        timeProvider.getCurrentTime,
        submitterInfo.commandId,
        submitterInfo.applicationId,
        submitterInfo.submitter,
        reason)
    )
    ()
  }

  override def close(): Unit = ()

  override def lookupTransaction(
      transactionId: TransactionIdString): Future[Option[(Long, LedgerEntry.Transaction)]] = {

    Try(transactionId.toLong) match {
      case Failure(_) =>
        Future.successful(None)
      case Success(n) =>
        Future.successful(
          entries
            .getEntryAt(n)
            .collect[(Long, LedgerEntry.Transaction)] {
              case t: LedgerEntry.Transaction =>
                (n, t) // the transaction id is also the offset
            })
    }
  }

  override def parties: Future[List[PartyDetails]] =
    Future.successful(this.synchronized {
      acs.parties.values.toList
    })

  override def allocateParty(
      party: Party,
      displayName: Option[String]): Future[PartyAllocationResult] =
    Future.successful(this.synchronized {
      val ids = acs.parties.keySet

      if (ids.contains(party))
        PartyAllocationResult.AlreadyExists
      else {
        val details = PartyDetails(party, displayName, true)
        acs = acs.addParty(details)
        PartyAllocationResult.Ok(details)
      }
    })

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    packageStoreRef.get.listLfPackages()

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    packageStoreRef.get.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    packageStoreRef.get.getLfPackage(packageId)

  override def uploadPackages(
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[UploadPackagesResult] = {
    val oldStore = packageStoreRef.get
    oldStore
      .withPackages(knownSince, sourceDescription, payload)
      .fold(
        err => Future.successful(UploadPackagesResult.InvalidPackage(err)),
        newStore => {
          if (packageStoreRef.compareAndSet(oldStore, newStore))
            Future.successful(UploadPackagesResult.Ok)
          else uploadPackages(knownSince, sourceDescription, payload)
        }
      )
  }
}
