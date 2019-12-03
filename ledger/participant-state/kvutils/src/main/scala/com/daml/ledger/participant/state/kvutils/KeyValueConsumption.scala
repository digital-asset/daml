// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.ledger.api.domain.PartyDetails
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

import scala.collection.breakOut
import scala.collection.JavaConverters._

/** Utilities for producing [[Update]] events from [[DamlLogEntry]]'s committed to a
  * key-value based ledger.
  */
object KeyValueConsumption {

  sealed trait AsyncResponse extends Serializable with Product
  final case class PartyAllocationResponse(submissionId: String, result: PartyAllocationResult)
      extends AsyncResponse
  final case class PackageUploadResponse(submissionId: String, result: UploadPackagesResult)
      extends AsyncResponse

  def packDamlLogEntry(entry: DamlStateKey): ByteString = entry.toByteString
  def unpackDamlLogEntry(bytes: ByteString): DamlLogEntry = DamlLogEntry.parseFrom(bytes)

  /** Construct participant-state [[Update]]s from a [[DamlLogEntry]].
    * Throws [[Err]] exception on badly formed data.
    *
    * This method is expected to be used to implement [[com.daml.ledger.participant.state.v1.ReadService.stateUpdates]].
    *
    * @param entryId: The log entry identifier.
    * @param entry: The log entry.
    * @return [[Update]]s constructed from log entry.
    */
  @throws(classOf[Err])
  def logEntryToUpdate(entryId: DamlLogEntryId, entry: DamlLogEntry): List[Update] = {
    val recordTime = parseTimestamp(entry.getRecordTime)

    entry.getPayloadCase match {
      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        entry.getPackageUploadEntry.getArchivesList.asScala.map { archive =>
          Update.PublicPackageUploaded(
            archive,
            if (entry.getPackageUploadEntry.getSourceDescription.nonEmpty)
              Some(entry.getPackageUploadEntry.getSourceDescription)
            else None,
            parseLedgerString("ParticipantId")(entry.getPackageUploadEntry.getParticipantId),
            recordTime,
            TracingInfo("")
          )
        }(breakOut)

      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY =>
        List.empty

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        val pae = entry.getPartyAllocationEntry
        val party = parseParty(pae.getParty)
        val participantId = parseLedgerString("ParticipantId")(pae.getParticipantId)
        List(
          Update.PartyAddedToParticipant(
            party,
            pae.getDisplayName,
            participantId,
            recordTime,
            TracingInfo(""))
        )

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        List.empty

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        List(txEntryToUpdate(entryId, entry.getTransactionEntry, recordTime))

      case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        val configEntry = entry.getConfigurationEntry
        val newConfig = Configuration
          .decode(configEntry.getConfiguration)
          .fold(err => throw Err.DecodeError("Configuration", err), identity)
        val participantId =
          parseLedgerString("ParticipantId")(configEntry.getParticipantId)
        List(
          Update.ConfigurationChanged(
            recordTime,
            configEntry.getSubmissionId,
            participantId,
            newConfig,
            TracingInfo("")
          )
        )

      case DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY =>
        val rejection = entry.getConfigurationRejectionEntry
        val proposedConfig = Configuration
          .decode(rejection.getConfiguration)
          .fold(err => throw Err.DecodeError("Configuration", err), identity)
        val participantId =
          parseLedgerString("ParticipantId")(rejection.getParticipantId)
        List(
          Update.ConfigurationChangeRejected(
            recordTime = recordTime,
            submissionId = rejection.getSubmissionId,
            participantId = participantId,
            proposedConfiguration = proposedConfig,
            rejectionReason = rejection.getReasonCase match {
              case DamlConfigurationRejectionEntry.ReasonCase.GENERATION_MISMATCH =>
                s"Generation mismatch: ${proposedConfig.generation} != ${rejection.getGenerationMismatch.getExpectedGeneration}"
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
            },
            TracingInfo("")
          ))

      case DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY =>
        List(
          transactionRejectionEntryToUpdate(recordTime, entry.getTransactionRejectionEntry)
        )

      case DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InternalError("logEntryToUpdate: PAYLOAD_NOT_SET!")
    }
  }

  /** Construct a participant-state [[AsyncResponse]] from a [[DamlLogEntry]].
    *
    * This method is expected to be used to implement [[com.daml.ledger.participant.state.v1.WriteService.allocateParty]]
    * and [[com.daml.ledger.participant.state.v1.WriteService.uploadPackages]], both of which require matching requests
    * with asynchronous responses.
    *
    * @param entryId: The log entry identifier.
    * @param entry: The log entry.
    * @return [[Update]] constructed from log entry.
    */
  def logEntryToAsyncResponse(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      participantId: String): Option[AsyncResponse] = {

    entry.getPayloadCase match {
      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        if (participantId == entry.getPackageUploadEntry.getParticipantId)
          Some(
            PackageUploadResponse(
              entry.getPackageUploadEntry.getSubmissionId,
              UploadPackagesResult.Ok
            )
          )
        else None

      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY =>
        if (participantId == entry.getPackageUploadRejectionEntry.getParticipantId)
          Some(packageRejectionEntryToAsyncResponse(entry.getPackageUploadRejectionEntry))
        else None

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        if (participantId == entry.getPartyAllocationEntry.getParticipantId)
          Some(
            PartyAllocationResponse(
              entry.getPartyAllocationEntry.getSubmissionId,
              PartyAllocationResult.Ok(
                PartyDetails(
                  parseParty(entry.getPartyAllocationEntry.getParty),
                  if (entry.getPartyAllocationEntry.getDisplayName.isEmpty)
                    None
                  else
                    Some(entry.getPartyAllocationEntry.getDisplayName),
                  entry.getPartyAllocationEntry.getParticipantId == participantId
                )
              )
            )
          )
        else None

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        if (participantId == entry.getPartyAllocationRejectionEntry.getParticipantId)
          Some(partyRejectionEntryToAsyncResponse(entry.getPartyAllocationRejectionEntry))
        else None

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        None

      case DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY =>
        None

      case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        None

      case DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY =>
        None

      case DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InternalError("logEntryToAsyncResponse: PAYLOAD_NOT_SET!")
    }
  }

  private def transactionRejectionEntryToUpdate(
      recordTime: Timestamp,
      rejEntry: DamlTransactionRejectionEntry): Update.CommandRejected =
    Update.CommandRejected(
      recordTime = recordTime,
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
          throw Err.InternalError("transactionRejectionEntryToUpdate: REASON_NOT_SET!")
      },
      TracingInfo("")
    )

  private def partyRejectionEntryToAsyncResponse(
      rejEntry: DamlPartyAllocationRejectionEntry): PartyAllocationResponse = {

    PartyAllocationResponse(
      submissionId = rejEntry.getSubmissionId,
      result = rejEntry.getReasonCase match {
        case DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME =>
          PartyAllocationResult.InvalidName(rejEntry.getInvalidName.getDetails)
        case DamlPartyAllocationRejectionEntry.ReasonCase.ALREADY_EXISTS =>
          PartyAllocationResult.AlreadyExists
        case DamlPartyAllocationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
          PartyAllocationResult.ParticipantNotAuthorized
        case DamlPartyAllocationRejectionEntry.ReasonCase.REASON_NOT_SET =>
          throw Err.InternalError("partyRejectionEntryToUpdate: REASON_NOT_SET!")
      }
    )
  }

  private def packageRejectionEntryToAsyncResponse(
      rejEntry: DamlPackageUploadRejectionEntry): PackageUploadResponse = {

    PackageUploadResponse(
      submissionId = rejEntry.getSubmissionId,
      result = rejEntry.getReasonCase match {
        case DamlPackageUploadRejectionEntry.ReasonCase.INVALID_PACKAGE =>
          UploadPackagesResult.InvalidPackage(rejEntry.getInvalidPackage.getDetails)
        case DamlPackageUploadRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
          UploadPackagesResult.ParticipantNotAuthorized
        case DamlPackageUploadRejectionEntry.ReasonCase.REASON_NOT_SET =>
          throw Err.InternalError("rejectionEntryToUpdate: REASON_NOT_SET!")
      }
    )
  }

  /** Transform the transaction entry into the [[Update.TransactionAccepted]] event. */
  private def txEntryToUpdate(
      entryId: DamlLogEntryId,
      txEntry: DamlTransactionEntry,
      recordTime: Timestamp): Update.TransactionAccepted = {
    val relTx = Conversions.decodeTransaction(txEntry.getTransaction)
    val hexTxId = parseLedgerString("TransactionId")(
      BaseEncoding.base16.encode(entryId.toByteArray)
    )
    Update.TransactionAccepted(
      optSubmitterInfo = Some(parseSubmitterInfo(txEntry.getSubmitterInfo)),
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = parseTimestamp(txEntry.getLedgerEffectiveTime),
        workflowId = Some(txEntry.getWorkflowId)
          .filter(_.nonEmpty)
          .map(parseLedgerString("WorkflowId")),
      ),
      transaction = makeCommittedTransaction(entryId, relTx),
      transactionId = hexTxId,
      recordTime = recordTime,
      divulgedContracts = List.empty,
      TracingInfo("")
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

  @throws(classOf[Err])
  private def parseLedgerString(what: String)(s: String): Ref.LedgerString =
    Ref.LedgerString
      .fromString(s)
      .fold(err => throw Err.DecodeError(what, "Cannot parse '$s': $err"), identity)

  @throws(classOf[Err])
  private def parseParty(s: String): Ref.Party =
    Ref.Party
      .fromString(s)
      .fold(err => throw Err.DecodeError("Party", "Cannot parse '$s': $err"), identity)

}
