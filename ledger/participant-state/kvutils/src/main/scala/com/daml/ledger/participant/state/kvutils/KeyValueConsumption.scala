// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._

/** Utilities for producing [[Update]] events from [[DamlLogEntry]]'s committed to a
  * key-value based ledger.
  */
object KeyValueConsumption {

  def packDamlLogEntry(entry: DamlStateKey): ByteString = entry.toByteString
  def unpackDamlLogEntry(bytes: ByteString): DamlLogEntry = DamlLogEntry.parseFrom(bytes)

  /** Construct a participant-state [[Update]] from a [[DamlLogEntry]].
    *
    * This method is expected to be used to implement [[com.daml.ledger.participant.state.v1.ReadService.stateUpdates]].
    *
    * @param entryId: The log entry identifier.
    * @param entry: The log entry.
    * @return [[[Update]] constructed from log entry.
    */
  def logEntryToUpdateOrResult(entryId: DamlLogEntryId, entry: DamlLogEntry): Update = {

    val recordTime = parseTimestamp(entry.getRecordTime)

    entry.getPayloadCase match {
      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        Update.PublicPackagesUploaded(
          entry.getPackageUploadEntry.getSubmissionId,
          entry.getPackageUploadEntry.getArchivesList.asScala.toList,
          entry.getPackageUploadEntry.getSourceDescription,
          entry.getPackageUploadEntry.getParticipantId,
          recordTime
        )

      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY =>
        Update.PackagesRejected(
          entry.getPackageUploadRejectionEntry.getSubmissionId,
          //TODO(MZ): Implement error conversion
          PackageUploadRejectionReason.InvalidPackage
        )

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        Update.PartyAddedToParticipant(
          entry.getPartyAllocationEntry.getSubmissionId,
          Party.assertFromString(entry.getPartyAllocationEntry.getParty),
          entry.getPartyAllocationEntry.getDisplayName,
          entry.getPartyAllocationEntry.getParticipantId,
          recordTime
        )

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        Update.PartyRejected(
          entry.getPartyAllocationRejectionEntry.getSubmissionId,
          //TODO(MZ): Implement error conversion
          PartyAllocationRejectionReason.InvalidName
        )

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        txEntryToUpdate(entryId, entry.getTransactionEntry, recordTime)

      case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        Update.ConfigurationChanged(parseDamlConfigurationEntry(entry.getConfigurationEntry))

      case DamlLogEntry.PayloadCase.REJECTION_ENTRY =>
        rejectionEntryToUpdate(entryId, entry.getRejectionEntry, recordTime)

      case DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET =>
        sys.error("entryToUpdate: PAYLOAD_NOT_SET!")
    }
  }

  private def rejectionEntryToUpdate(
      entryId: DamlLogEntryId,
      rejEntry: DamlRejectionEntry,
      recordTime: Timestamp): Update.CommandRejected = {

    Update.CommandRejected(
      submitterInfo = parseSubmitterInfo(rejEntry.getSubmitterInfo),
      reason = rejEntry.getReasonCase match {
        case DamlRejectionEntry.ReasonCase.DISPUTED =>
          RejectionReason.Disputed(rejEntry.getDisputed)
        case DamlRejectionEntry.ReasonCase.INCONSISTENT =>
          RejectionReason.Inconsistent
        case DamlRejectionEntry.ReasonCase.RESOURCES_EXHAUSTED =>
          RejectionReason.ResourcesExhausted
        case DamlRejectionEntry.ReasonCase.MAXIMUM_RECORD_TIME_EXCEEDED =>
          RejectionReason.MaximumRecordTimeExceeded
        case DamlRejectionEntry.ReasonCase.DUPLICATE_COMMAND =>
          RejectionReason.DuplicateCommand
        case DamlRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER =>
          RejectionReason.PartyNotKnownOnLedger
        case DamlRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT =>
          RejectionReason.SubmitterCannotActViaParticipant(
            rejEntry.getSubmitterCannotActViaParticipant
          )
        case DamlRejectionEntry.ReasonCase.REASON_NOT_SET =>
          sys.error("rejectionEntryToUpdate: REASON_NOT_SET!")
      }
    )
  }

  /** Transform the transaction entry into the [[Update.TransactionAccepted]] event. */
  private def txEntryToUpdate(
      entryId: DamlLogEntryId,
      txEntry: DamlTransactionEntry,
      recordTime: Timestamp): Update.TransactionAccepted = {
    val relTx = Conversions.decodeTransaction(txEntry.getTransaction)
    val hexTxId = LedgerString.assertFromString(BaseEncoding.base16.encode(entryId.toByteArray))

    Update.TransactionAccepted(
      optSubmitterInfo = Some(parseSubmitterInfo(txEntry.getSubmitterInfo)),
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = parseTimestamp(txEntry.getLedgerEffectiveTime),
        workflowId =
          Some(txEntry.getWorkflowId).filter(_.nonEmpty).map(LedgerString.assertFromString),
      ),
      transaction = makeCommittedTransaction(entryId, relTx),
      transactionId = hexTxId,
      recordTime = recordTime,
      referencedContracts = List.empty // TODO(JM): rename this to additionalContracts. Always empty here.
    )
  }

  private def makeCommittedTransaction(
      txId: DamlLogEntryId,
      tx: SubmittedTransaction): CommittedTransaction = {
    tx
    /* Assign absolute contract ids */
      .mapContractIdAndValue(
        toAbsCoid(txId, _),
        _.mapContractId(toAbsCoid(txId, _))
      )
  }

}
