// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.Update.PartyAllocationEntryRejected
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._

/** Utilities for producing [[Update]] events from [[DamlLogEntry]]'s committed to a
  * key-value based ledger.
  */
object KeyValueConsumption {

  sealed trait AsyncResponse extends Serializable with Product

  def packDamlLogEntry(entry: DamlStateKey): ByteString = entry.toByteString
  def unpackDamlLogEntry(bytes: ByteString): DamlLogEntry = DamlLogEntry.parseFrom(bytes)

  /** Construct a participant-state [[Update]] from a [[DamlLogEntry]].
    *
    * This method is expected to be used to implement [[com.daml.ledger.participant.state.v1.ReadService.stateUpdates]].
    *
    * @param entryId: The log entry identifier.
    * @param entry: The log entry.
    * @return [[Update]] constructed from log entry.
    */
  // TODO add participantId to ensure participant id matches in DamlLogEntry
  def logEntryToUpdate(entryId: DamlLogEntryId, entry: DamlLogEntry): List[Update] = {

    val recordTime = parseTimestamp(entry.getRecordTime)

    entry.getPayloadCase match {
      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        val participantId =
          Ref.LedgerString.assertFromString(entry.getPackageUploadEntry.getParticipantId)
        val submissionId =
          Ref.LedgerString.assertFromString(entry.getPackageUploadEntry.getSubmissionId)
        //only send package uploaded update if the log entry includes an archive
        val uniquePackages = entry.getPackageUploadEntry.getArchivesList.asScala.map { archive =>
          Update.PublicPackageUploaded(
            archive,
            if (entry.getPackageUploadEntry.getSourceDescription.nonEmpty)
              Some(entry.getPackageUploadEntry.getSourceDescription)
            else None,
            participantId,
            recordTime,
            submissionId
          )
        }.toList

        // always send accepted message even if there are no package uploads
        // TODO only send accept message if participantId matches
        uniquePackages ++ List(
          Update
            .PackageUploadEntryAccepted(participantId, recordTime, submissionId))

      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY =>
        val rejection = entry.getPackageUploadRejectionEntry
        val participantId = rejection.getParticipantId
        val proposedPackageUpload = rejection.getInvalidPackage

        List(
          // TODO only send reject message if participantId matches
          Update.PackageUploadEntryRejected(
            Ref.LedgerString.assertFromString(participantId),
            recordTime,
            Ref.LedgerString.assertFromString(rejection.getSubmissionId),
            reason = rejection.getReasonCase match {
              case DamlPackageUploadRejectionEntry.ReasonCase.INVALID_PACKAGE =>
                s"Package ${proposedPackageUpload.getDetails} rejected as invalid"
              case DamlPackageUploadRejectionEntry.ReasonCase.REASON_NOT_SET =>
                s"Package ${proposedPackageUpload.getDetails} upload failed for undetermined reason"
              case DamlPackageUploadRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
                s"Participant $participantId not authorized to upload package"
            }
          ))

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        //TODO check for participant match
        //(participantId == entry.getPartyAllocationEntry.getParticipantId)
        List(
          Update.PartyAddedToParticipant(
            Party.assertFromString(entry.getPartyAllocationEntry.getParty),
            entry.getPartyAllocationEntry.getDisplayName,
            Ref.LedgerString.assertFromString(entry.getPartyAllocationEntry.getParticipantId),
            recordTime,
            Ref.LedgerString.assertFromString(entry.getPartyAllocationEntry.getSubmissionId)
            // TODO BH add isLocal with check
            // entry.getPartyAllocationEntry.getParticipantId == participantId
          )
        )

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        val rejection = entry.getPartyAllocationRejectionEntry
        val participantId = Ref.LedgerString.assertFromString(rejection.getParticipantId)

        //TODO BH: only send for matching participant who sent request
        // if (participantId == entry.getPartyAllocationRejectionEntry.getParticipantId)
        List(
          PartyAllocationEntryRejected(
            Ref.LedgerString.assertFromString(rejection.getSubmissionId),
            participantId,
            recordTime,
            rejection.getReasonCase match {
              case DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME =>
                s"Party name is invalid, details=${rejection.getInvalidName.getDetails}"
              case DamlPartyAllocationRejectionEntry.ReasonCase.ALREADY_EXISTS =>
                "Party already exists"
              case DamlPartyAllocationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
                "Participant is not authorized to allocate a party"
              case DamlPartyAllocationRejectionEntry.ReasonCase.REASON_NOT_SET =>
                sys.error("partyRejectionEntryToUpdate: REASON_NOT_SET!")
            }
          )
        )

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        List(txEntryToUpdate(entryId, entry.getTransactionEntry, recordTime))

      case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        val configEntry = entry.getConfigurationEntry
        val newConfig = parseDamlConfiguration(configEntry.getConfiguration).get
        List(Update.ConfigurationChanged(configEntry.getSubmissionId, newConfig))

      case DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY =>
        val rejection = entry.getConfigurationRejectionEntry
        val proposedConfig = rejection.getConfiguration
        List(
          Update.ConfigurationChangeRejected(
            submissionId = rejection.getSubmissionId,
            reason = rejection.getReasonCase match {
              case DamlConfigurationRejectionEntry.ReasonCase.GENERATION_MISMATCH =>
                s"Generation mismatch: ${proposedConfig.getGeneration} != ${rejection.getGenerationMismatch.getExpectedGeneration}"
              case DamlConfigurationRejectionEntry.ReasonCase.INVALID_CONFIGURATION =>
                s"Invalid configuration: ${rejection.getInvalidConfiguration.getError}"
              case DamlConfigurationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
                s"Participant not authorized to modify configuration"
              case DamlConfigurationRejectionEntry.ReasonCase.TIMED_OUT =>
                val timedOut = rejection.getTimedOut
                val mrt = Conversions.parseTimestamp(timedOut.getMaximumRecordTime)
                val rt = Conversions.parseTimestamp(timedOut.getRecordTime)
                s"Configuration change request timed out: $mrt > $rt"
              case DamlConfigurationRejectionEntry.ReasonCase.REASON_NOT_SET =>
                "Unknown reason"
            }
          ))

      case DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY =>
        List(transactionRejectionEntryToUpdate(entry.getTransactionRejectionEntry))

      case DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET =>
        sys.error("entryToUpdate: PAYLOAD_NOT_SET!")
    }
  }

  private def transactionRejectionEntryToUpdate(
      rejEntry: DamlTransactionRejectionEntry): Update.CommandRejected = {

    Update.CommandRejected(
      submitterInfo = parseSubmitterInfo(rejEntry.getSubmitterInfo),
      reason = rejEntry.getReasonCase match {
        case DamlTransactionRejectionEntry.ReasonCase.DISPUTED =>
          RejectionReason.Disputed(rejEntry.getDisputed.getDetails)
        case DamlTransactionRejectionEntry.ReasonCase.INCONSISTENT =>
          RejectionReason.Inconsistent
        case DamlTransactionRejectionEntry.ReasonCase.RESOURCES_EXHAUSTED =>
          RejectionReason.ResourcesExhausted
        case DamlTransactionRejectionEntry.ReasonCase.MAXIMUM_RECORD_TIME_EXCEEDED =>
          RejectionReason.MaximumRecordTimeExceeded
        case DamlTransactionRejectionEntry.ReasonCase.DUPLICATE_COMMAND =>
          RejectionReason.DuplicateCommand
        case DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER =>
          RejectionReason.PartyNotKnownOnLedger
        case DamlTransactionRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT =>
          RejectionReason.SubmitterCannotActViaParticipant(
            rejEntry.getSubmitterCannotActViaParticipant.getDetails
          )
        case DamlTransactionRejectionEntry.ReasonCase.REASON_NOT_SET =>
          sys.error("transactionRejectionEntryToUpdate: REASON_NOT_SET!")
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
      divulgedContracts = List.empty
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
