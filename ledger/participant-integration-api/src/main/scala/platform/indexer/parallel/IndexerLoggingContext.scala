// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.time.Duration

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.entries.{LoggingEntries, LoggingEntry}

object IndexerLoggingContext {
  import state.Update._
  import state._

  def loggingEntriesFor(
      offset: Offset,
      update: Update,
  ): LoggingEntries =
    loggingEntriesFor(update) :+
      "updateRecordTime" -> update.recordTime.toInstant :+
      "updateOffset" -> offset

  private def loggingEntriesFor(update: Update): LoggingEntries =
    update match {
      case ConfigurationChanged(_, submissionId, participantId, newConfiguration) =>
        LoggingEntries(
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.configGeneration(newConfiguration.generation),
          Logging.maxDeduplicationTime(newConfiguration.maxDeduplicationTime),
        )
      case ConfigurationChangeRejected(
            _,
            submissionId,
            participantId,
            proposedConfiguration,
            rejectionReason,
          ) =>
        LoggingEntries(
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.configGeneration(proposedConfiguration.generation),
          Logging.maxDeduplicationTime(proposedConfiguration.maxDeduplicationTime),
          Logging.rejectionReason(rejectionReason),
        )
      case PartyAddedToParticipant(party, displayName, participantId, _, submissionId) =>
        LoggingEntries(
          Logging.submissionIdOpt(submissionId),
          Logging.participantId(participantId),
          Logging.party(party),
          Logging.displayName(displayName),
        )
      case PartyAllocationRejected(submissionId, participantId, _, rejectionReason) =>
        LoggingEntries(
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.rejectionReason(rejectionReason),
        )
      case PublicPackageUpload(_, sourceDescription, _, submissionId) =>
        LoggingEntries(
          Logging.submissionIdOpt(submissionId),
          Logging.sourceDescriptionOpt(sourceDescription),
        )
      case PublicPackageUploadRejected(submissionId, _, rejectionReason) =>
        LoggingEntries(
          Logging.submissionId(submissionId),
          Logging.rejectionReason(rejectionReason),
        )
      case TransactionAccepted(optSubmitterInfo, transactionMeta, _, transactionId, _, _, _) =>
        LoggingEntries(
          Logging.transactionId(transactionId),
          Logging.ledgerTime(transactionMeta.ledgerEffectiveTime),
          Logging.workflowIdOpt(transactionMeta.workflowId),
          Logging.submissionTime(transactionMeta.submissionTime),
        ) ++ optSubmitterInfo
          .map(info =>
            LoggingEntries(
              Logging.submitter(info.actAs),
              Logging.applicationId(info.applicationId),
              Logging.commandId(info.commandId),
              Logging.deduplicationPeriod(info.optDeduplicationPeriod),
            )
          )
          .getOrElse(LoggingEntries.empty)
      case CommandRejected(_, submitterInfo, reason) =>
        LoggingEntries(
          Logging.submitter(submitterInfo.actAs),
          Logging.applicationId(submitterInfo.applicationId),
          Logging.commandId(submitterInfo.commandId),
          Logging.deduplicationPeriod(submitterInfo.optDeduplicationPeriod),
          Logging.rejectionReason(reason),
        )
    }

  private object Logging {
    def submissionId(id: Ref.SubmissionId): LoggingEntry =
      "submissionId" -> id

    def submissionIdOpt(id: Option[Ref.SubmissionId]): LoggingEntry =
      "submissionId" -> id

    def participantId(id: Ref.ParticipantId): LoggingEntry =
      "participantId" -> id

    def commandId(id: Ref.CommandId): LoggingEntry =
      "commandId" -> id

    def party(party: Ref.Party): LoggingEntry =
      "party" -> party

    def transactionId(id: Ref.TransactionId): LoggingEntry =
      "transactionId" -> id

    def applicationId(id: Ref.ApplicationId): LoggingEntry =
      "applicationId" -> id

    def workflowIdOpt(id: Option[Ref.WorkflowId]): LoggingEntry =
      "workflowId" -> id

    def ledgerTime(time: Timestamp): LoggingEntry =
      "ledgerTime" -> time.toInstant

    def submissionTime(time: Timestamp): LoggingEntry =
      "submissionTime" -> time.toInstant

    def configGeneration(generation: Long): LoggingEntry =
      "configGeneration" -> generation

    def maxDeduplicationTime(time: Duration): LoggingEntry =
      "maxDeduplicationTime" -> time

    def deduplicationPeriod(period: Option[state.DeduplicationPeriod]): LoggingEntry =
      "deduplicationPeriod" -> period

    def rejectionReason(rejectionReason: String): LoggingEntry =
      "rejectionReason" -> rejectionReason

    def rejectionReason(
        rejectionReasonTemplate: state.Update.CommandRejected.RejectionReasonTemplate
    ): LoggingEntry =
      "rejectionReason" -> rejectionReasonTemplate

    def displayName(name: String): LoggingEntry =
      "displayName" -> name

    def sourceDescriptionOpt(description: Option[String]): LoggingEntry =
      "sourceDescription" -> description

    def submitter(parties: List[Ref.Party]): LoggingEntry =
      "submitter" -> parties
  }

}
