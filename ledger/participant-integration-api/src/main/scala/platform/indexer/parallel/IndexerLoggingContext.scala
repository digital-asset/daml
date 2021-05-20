// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.v1.Update.{
  CommandRejected,
  ConfigurationChangeRejected,
  ConfigurationChanged,
  PartyAddedToParticipant,
  PartyAllocationRejected,
  PublicPackageUpload,
  PublicPackageUploadRejected,
  TransactionAccepted,
}
import com.daml.ledger.participant.state.v1.{
  ApplicationId,
  CommandId,
  Offset,
  ParticipantId,
  Party,
  SubmissionId,
  TransactionId,
  Update,
  WorkflowId,
}
import com.daml.lf.data.Time.Timestamp

object IndexerLoggingContext {
  def loggingContextFor(
      offset: Offset,
      update: Update,
  ): Map[String, String] =
    loggingContextFor(update)
      .updated("updateRecordTime", update.recordTime.toInstant.toString)
      .updated("updateOffset", offset.toHexString)

  private def loggingContextFor(update: Update): Map[String, String] =
    update match {
      case ConfigurationChanged(_, submissionId, participantId, newConfiguration) =>
        Map(
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
        Map(
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.configGeneration(proposedConfiguration.generation),
          Logging.maxDeduplicationTime(proposedConfiguration.maxDeduplicationTime),
          Logging.rejectionReason(rejectionReason),
        )
      case PartyAddedToParticipant(party, displayName, participantId, _, submissionId) =>
        Map(
          Logging.submissionIdOpt(submissionId),
          Logging.participantId(participantId),
          Logging.party(party),
          Logging.displayName(displayName),
        )
      case PartyAllocationRejected(submissionId, participantId, _, rejectionReason) =>
        Map(
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.rejectionReason(rejectionReason),
        )
      case PublicPackageUpload(_, sourceDescription, _, submissionId) =>
        Map(
          Logging.submissionIdOpt(submissionId),
          Logging.sourceDescriptionOpt(sourceDescription),
        )
      case PublicPackageUploadRejected(submissionId, _, rejectionReason) =>
        Map(
          Logging.submissionId(submissionId),
          Logging.rejectionReason(rejectionReason),
        )
      case TransactionAccepted(optSubmitterInfo, transactionMeta, _, transactionId, _, _, _) =>
        Map(
          Logging.transactionId(transactionId),
          Logging.ledgerTime(transactionMeta.ledgerEffectiveTime),
          Logging.workflowIdOpt(transactionMeta.workflowId),
          Logging.submissionTime(transactionMeta.submissionTime),
        ) ++ optSubmitterInfo
          .map(info =>
            Map(
              Logging.submitter(info.actAs),
              Logging.applicationId(info.applicationId),
              Logging.commandId(info.commandId),
              Logging.deduplicateUntil(info.deduplicateUntil),
            )
          )
          .getOrElse(Map.empty)
      case CommandRejected(_, submitterInfo, reason) =>
        Map(
          Logging.submitter(submitterInfo.actAs),
          Logging.applicationId(submitterInfo.applicationId),
          Logging.commandId(submitterInfo.commandId),
          Logging.deduplicateUntil(submitterInfo.deduplicateUntil),
          Logging.rejectionReason(reason.description),
        )
    }

  private object Logging {
    def submissionId(id: SubmissionId): (String, String) =
      "submissionId" -> id
    def submissionIdOpt(id: Option[SubmissionId]): (String, String) =
      "submissionId" -> id.getOrElse("")
    def participantId(id: ParticipantId): (String, String) =
      "participantId" -> id
    def commandId(id: CommandId): (String, String) =
      "commandId" -> id
    def party(party: Party): (String, String) =
      "party" -> party
    def transactionId(id: TransactionId): (String, String) =
      "transactionId" -> id
    def applicationId(id: ApplicationId): (String, String) =
      "applicationId" -> id
    def workflowIdOpt(id: Option[WorkflowId]): (String, String) =
      "workflowId" -> id.getOrElse("")
    def ledgerTime(time: Timestamp): (String, String) =
      "ledgerTime" -> time.toInstant.toString
    def submissionTime(time: Timestamp): (String, String) =
      "submissionTime" -> time.toInstant.toString
    def configGeneration(generation: Long): (String, String) =
      "configGeneration" -> generation.toString
    def maxDeduplicationTime(time: Duration): (String, String) =
      "maxDeduplicationTime" -> time.toString
    def deduplicateUntil(time: Instant): (String, String) =
      "deduplicateUntil" -> time.toString
    def rejectionReason(reason: String): (String, String) =
      "rejectionReason" -> reason
    def displayName(name: String): (String, String) =
      "displayName" -> name
    def sourceDescriptionOpt(description: Option[String]): (String, String) =
      "sourceDescription" -> description.getOrElse("")
    def submitter(parties: List[Party]): (String, String) =
      "submitter" -> parties.mkString("[", ", ", "]")
  }

}
