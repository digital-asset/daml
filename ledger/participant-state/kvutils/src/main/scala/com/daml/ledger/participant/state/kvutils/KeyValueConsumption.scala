// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.CommittedTransaction
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Utilities for producing [[Update]] events from [[DamlLogEntry]]'s committed to a
  * key-value based ledger.
  */
object KeyValueConsumption {
  private val logger = LoggerFactory.getLogger(this.getClass)

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
  // TODO(BH): add participantId to ensure participant id matches in DamlLogEntry
  @throws(classOf[Err])
  def logEntryToUpdate(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      recordTimeForUpdate: Option[Timestamp] = None): List[Update] = {
    val recordTimeFromLogEntry = PartialFunction.condOpt(entry.hasRecordTime) {
      case true => parseTimestamp(entry.getRecordTime)
    }
    val recordTime = resolveRecordTimeOrThrow(recordTimeForUpdate, recordTimeFromLogEntry)

    entry.getPayloadCase match {
      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        val pue = entry.getPackageUploadEntry
        List(
          Update.PublicPackageUpload(
            pue.getArchivesList.asScala.toList,
            if (entry.getPackageUploadEntry.getSourceDescription.nonEmpty)
              Some(entry.getPackageUploadEntry.getSourceDescription)
            else None,
            recordTime,
            if (pue.getSubmissionId.nonEmpty)
              Some(parseLedgerString("SubmissionId")(pue.getSubmissionId))
            else None,
          )
        )

      case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY =>
        val pur = entry.getPackageUploadRejectionEntry
        def wrap(reason: String) =
          List(
            Update.PublicPackageUploadRejected(
              parseLedgerString("SubmissionId")(pur.getSubmissionId),
              recordTime,
              reason))

        pur.getReasonCase match {
          case DamlPackageUploadRejectionEntry.ReasonCase.INVALID_PACKAGE =>
            wrap(s"Invalid package, details=${pur.getInvalidPackage.getDetails}")
          case DamlPackageUploadRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
            wrap(s"Participant is not authorized to upload packages")
          // Ignore rejections due to duplicates
          case DamlPackageUploadRejectionEntry.ReasonCase.DUPLICATE_SUBMISSION =>
            List()
          case DamlPackageUploadRejectionEntry.ReasonCase.REASON_NOT_SET =>
            wrap("Unknown reason")
        }

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        // TODO(BH): add isLocal with check:
        // entry.getPartyAllocationEntry.getParticipantId == participantId
        val pae = entry.getPartyAllocationEntry
        val party = parseParty(pae.getParty)
        val participantId = parseParticipantId("ParticipantId")(pae.getParticipantId)
        val submissionId =
          Option(pae.getSubmissionId).filterNot(_.isEmpty).map(parseLedgerString("SubmissionId"))
        List(
          Update.PartyAddedToParticipant(
            party,
            pae.getDisplayName,
            participantId,
            recordTime,
            submissionId))

      case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        val rejection = entry.getPartyAllocationRejectionEntry
        val participantId = parseParticipantId("ParticipantId")(rejection.getParticipantId)
        val submissionId = parseLedgerString("SubmissionId")(rejection.getSubmissionId)
        def wrap(reason: String) =
          List(Update.PartyAllocationRejected(submissionId, participantId, recordTime, reason))

        // TODO(BH): only send for matching participant who sent request
        // if (participantId == entry.getPartyAllocationRejectionEntry.getParticipantId)
        rejection.getReasonCase match {
          case DamlPartyAllocationRejectionEntry.ReasonCase.INVALID_NAME =>
            wrap(s"Party name is invalid, details=${rejection.getInvalidName.getDetails}")
          case DamlPartyAllocationRejectionEntry.ReasonCase.ALREADY_EXISTS =>
            wrap("Party already exists")
          case DamlPartyAllocationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
            wrap("Participant is not authorized to allocate a party")
          // Ignore rejections due to duplicates
          case DamlPartyAllocationRejectionEntry.ReasonCase.DUPLICATE_SUBMISSION =>
            List()
          case DamlPartyAllocationRejectionEntry.ReasonCase.REASON_NOT_SET =>
            wrap("Unknown reason")
        }

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        List(transactionEntryToUpdate(entryId, entry.getTransactionEntry, recordTime))

      case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        val configEntry = entry.getConfigurationEntry
        val newConfig = Configuration
          .decode(configEntry.getConfiguration)
          .fold(err => throw Err.DecodeError("Configuration", err), identity)
        val participantId =
          parseParticipantId("ParticipantId")(configEntry.getParticipantId)
        val submissionId =
          parseLedgerString("SubmissionId")(configEntry.getSubmissionId)
        List(
          Update.ConfigurationChanged(
            recordTime,
            submissionId,
            participantId,
            newConfig
          )
        )

      case DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY =>
        val rejection = entry.getConfigurationRejectionEntry
        val proposedConfig = Configuration
          .decode(rejection.getConfiguration)
          .fold(err => throw Err.DecodeError("Configuration", err), identity)
        val participantId =
          parseParticipantId("ParticipantId")(rejection.getParticipantId)
        val submissionId =
          parseLedgerString("SubmissionId")(rejection.getSubmissionId)
        def wrap(reason: String) =
          List(
            Update.ConfigurationChangeRejected(
              recordTime = recordTime,
              submissionId = submissionId,
              participantId = participantId,
              proposedConfiguration = proposedConfig,
              reason))

        rejection.getReasonCase match {
          case DamlConfigurationRejectionEntry.ReasonCase.GENERATION_MISMATCH =>
            wrap(
              s"Generation mismatch: ${proposedConfig.generation} != ${rejection.getGenerationMismatch.getExpectedGeneration}")
          case DamlConfigurationRejectionEntry.ReasonCase.INVALID_CONFIGURATION =>
            wrap(s"Invalid configuration: ${rejection.getInvalidConfiguration.getDetails}")
          case DamlConfigurationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED =>
            wrap(s"Participant not authorized to modify configuration")
          case DamlConfigurationRejectionEntry.ReasonCase.TIMED_OUT =>
            val timedOut = rejection.getTimedOut
            val mrt = Conversions.parseTimestamp(timedOut.getMaximumRecordTime)
            val rt = Conversions.parseTimestamp(timedOut.getRecordTime)
            wrap(s"Configuration change timed out: $mrt > $rt")
          // Ignore rejections due to duplicates
          case DamlConfigurationRejectionEntry.ReasonCase.DUPLICATE_SUBMISSION =>
            List()
          case DamlConfigurationRejectionEntry.ReasonCase.REASON_NOT_SET =>
            wrap("Unknown reason")
        }

      case DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY =>
        transactionRejectionEntryToUpdate(recordTime, entry.getTransactionRejectionEntry)

      case DamlLogEntry.PayloadCase.OUT_OF_TIME_BOUNDS_ENTRY =>
        outOfTimeBoundsEntryToUpdate(recordTime, entry.getOutOfTimeBoundsEntry).toList

      case DamlLogEntry.PayloadCase.TIME_UPDATE_ENTRY =>
        List.empty

      case DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InternalError("logEntryToUpdate: PAYLOAD_NOT_SET!")
    }
  }

  private def resolveRecordTimeOrThrow(
      recordTimeForUpdate: Option[Timestamp],
      recordTimeFromLogEntry: Option[Timestamp]): Timestamp =
    (recordTimeForUpdate, recordTimeFromLogEntry) match {
      case (_, Some(recordTime)) => recordTime
      case (Some(recordTime), _) => recordTime
      case (None, None) =>
        throw Err.InternalError("Record time must be provided in order to generate an update")
    }

  private def transactionRejectionEntryToUpdate(
      recordTime: Timestamp,
      rejEntry: DamlTransactionRejectionEntry): List[Update] = {
    def wrap(reason: RejectionReason) =
      List(
        Update.CommandRejected(
          recordTime = recordTime,
          submitterInfo = parseSubmitterInfo(rejEntry.getSubmitterInfo),
          reason = reason))

    rejEntry.getReasonCase match {
      case DamlTransactionRejectionEntry.ReasonCase.DISPUTED =>
        wrap(RejectionReason.Disputed(rejEntry.getDisputed.getDetails))
      case DamlTransactionRejectionEntry.ReasonCase.INCONSISTENT =>
        wrap(RejectionReason.Inconsistent(rejEntry.getInconsistent.getDetails))
      case DamlTransactionRejectionEntry.ReasonCase.RESOURCES_EXHAUSTED =>
        wrap(RejectionReason.ResourcesExhausted(rejEntry.getResourcesExhausted.getDetails))
      case DamlTransactionRejectionEntry.ReasonCase.DUPLICATE_COMMAND =>
        List()
      case DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER =>
        wrap(RejectionReason.PartyNotKnownOnLedger(rejEntry.getPartyNotKnownOnLedger.getDetails))
      case DamlTransactionRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT =>
        wrap(
          RejectionReason.SubmitterCannotActViaParticipant(
            rejEntry.getSubmitterCannotActViaParticipant.getDetails
          ))
      case DamlTransactionRejectionEntry.ReasonCase.INVALID_LEDGER_TIME =>
        wrap(RejectionReason.InvalidLedgerTime(rejEntry.getInvalidLedgerTime.getDetails))
      case DamlTransactionRejectionEntry.ReasonCase.REASON_NOT_SET =>
        //TODO: Replace with "Unknown reason" error code or something similar
        throw Err.InternalError("transactionRejectionEntryToUpdate: REASON_NOT_SET!")
    }
  }

  /** Transform the transaction entry into the [[Update.TransactionAccepted]] event. */
  private def transactionEntryToUpdate(
      entryId: DamlLogEntryId,
      txEntry: DamlTransactionEntry,
      recordTime: Timestamp,
  ): Update.TransactionAccepted = {
    val transaction = Conversions.decodeTransaction(txEntry.getTransaction)
    val hexTxId = parseLedgerString("TransactionId")(
      BaseEncoding.base16.encode(entryId.toByteArray)
    )
    Update.TransactionAccepted(
      optSubmitterInfo =
        if (txEntry.hasSubmitterInfo) Some(parseSubmitterInfo(txEntry.getSubmitterInfo)) else None,
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = parseTimestamp(txEntry.getLedgerEffectiveTime),
        workflowId = Some(txEntry.getWorkflowId)
          .filter(_.nonEmpty)
          .map(parseLedgerString("WorkflowId")),
        submissionTime = parseTimestamp(txEntry.getSubmissionTime),
        submissionSeed = parseHash(txEntry.getSubmissionSeed),
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = CommittedTransaction(transaction),
      transactionId = hexTxId,
      recordTime = recordTime,
      divulgedContracts = List.empty,
      blindingInfo =
        if (txEntry.hasBlindingInfo)
          Some(Conversions.decodeBlindingInfo(txEntry.getBlindingInfo))
        else
          None,
    )
  }

  private[kvutils] case class TimeBounds(
      tooEarlyUntil: Option[Timestamp] = None,
      tooLateFrom: Option[Timestamp] = None,
      deduplicateUntil: Option[Timestamp] = None)

  private[kvutils] def outOfTimeBoundsEntryToUpdate(
      recordTime: Timestamp,
      outOfTimeBoundsEntry: DamlOutOfTimeBoundsEntry): Option[Update] = {
    val timeBounds = parseTimeBounds(outOfTimeBoundsEntry)
    val deduplicated = timeBounds.deduplicateUntil.exists(recordTime <= _)
    val tooEarly = timeBounds.tooEarlyUntil.exists(recordTime < _)
    val tooLate = timeBounds.tooLateFrom.exists(recordTime > _)
    val invalidRecordTime = tooEarly || tooLate

    val wrappedLogEntry = outOfTimeBoundsEntry.getEntry
    wrappedLogEntry.getPayloadCase match {
      case _ if deduplicated =>
        // We don't emit updates for deduplicated submissions.
        None

      case DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY if invalidRecordTime =>
        val transactionRejectionEntry = wrappedLogEntry.getTransactionRejectionEntry
        val reason = (timeBounds.tooEarlyUntil, timeBounds.tooLateFrom) match {
          case (Some(lowerBound), Some(upperBound)) =>
            s"Record time $recordTime outside of range [$lowerBound, $upperBound]"
          case (Some(lowerBound), None) =>
            s"Record time $recordTime  outside of valid range ($recordTime < $lowerBound)"
          case (None, Some(upperBound)) =>
            s"Record time $recordTime  outside of valid range ($recordTime > $upperBound)"
          case _ =>
            "Record time outside of valid range"
        }
        val rejectionReason = RejectionReason.InvalidLedgerTime(reason)
        Some(
          Update.CommandRejected(
            recordTime = recordTime,
            submitterInfo = parseSubmitterInfo(transactionRejectionEntry.getSubmitterInfo),
            reason = rejectionReason
          )
        )

      case DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY if invalidRecordTime =>
        val configurationRejectionEntry = wrappedLogEntry.getConfigurationRejectionEntry
        val reason = timeBounds.tooLateFrom
          .map { maximumRecordTime =>
            s"Configuration change timed out: $maximumRecordTime < $recordTime"
          }
          .getOrElse("Configuration change timed out")
        Some(
          Update.ConfigurationChangeRejected(
            recordTime,
            SubmissionId.assertFromString(configurationRejectionEntry.getSubmissionId),
            ParticipantId.assertFromString(configurationRejectionEntry.getParticipantId),
            Configuration.decode(configurationRejectionEntry.getConfiguration).right.get,
            reason
          )
        )

      case DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY |
          DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY |
          DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY |
          DamlLogEntry.PayloadCase.PARTY_ALLOCATION_REJECTION_ENTRY =>
        logger.error(
          s"Dropped out-of-time-bounds log entry of type=${wrappedLogEntry.getPayloadCase}")
        None

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY |
          DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY |
          DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY |
          DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY |
          DamlLogEntry.PayloadCase.OUT_OF_TIME_BOUNDS_ENTRY =>
        throw Err.InternalError(
          s"Out-of-time-bounds log entry does not contain a rejection entry: ${wrappedLogEntry.getPayloadCase}")
    }
  }

  private def parseTimeBounds(outOfTimeBoundsEntry: DamlOutOfTimeBoundsEntry): TimeBounds = {
    val duplicateUntilMaybe = parseOptionalTimestamp(
      outOfTimeBoundsEntry.hasDuplicateUntil,
      outOfTimeBoundsEntry.getDuplicateUntil)
    val tooEarlyUntilMaybe = parseOptionalTimestamp(
      outOfTimeBoundsEntry.hasTooEarlyUntil,
      outOfTimeBoundsEntry.getTooEarlyUntil)
    val tooLateFromMaybe = parseOptionalTimestamp(
      outOfTimeBoundsEntry.hasTooLateFrom,
      outOfTimeBoundsEntry.getTooLateFrom)
    TimeBounds(tooEarlyUntilMaybe, tooLateFromMaybe, duplicateUntilMaybe)
  }

  private def parseOptionalTimestamp(
      hasTimestamp: Boolean,
      getTimestamp: => com.google.protobuf.Timestamp): Option[Timestamp] =
    PartialFunction.condOpt(hasTimestamp) {
      case true => parseTimestamp(getTimestamp)
    }

  @throws(classOf[Err])
  private def parseLedgerString(what: String)(s: String): Ref.LedgerString =
    Ref.LedgerString
      .fromString(s)
      .fold(err => throw Err.DecodeError(what, s"Cannot parse '$s': $err"), identity)

  @throws(classOf[Err])
  private def parseParticipantId(what: String)(s: String): Ref.ParticipantId =
    Ref.ParticipantId
      .fromString(s)
      .fold(err => throw Err.DecodeError(what, s"Cannot parse '$s': $err"), identity)

  @throws(classOf[Err])
  private def parseParty(s: String): Ref.Party =
    Ref.Party
      .fromString(s)
      .fold(err => throw Err.DecodeError("Party", s"Cannot parse '$s': $err"), identity)

}
