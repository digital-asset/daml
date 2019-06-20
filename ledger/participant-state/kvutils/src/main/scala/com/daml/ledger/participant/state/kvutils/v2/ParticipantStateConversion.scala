package com.daml.ledger.participant.state.kvutils.v2

import java.time.Duration
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v2._
import com.daml.ledger.participant.state.{v1, v2}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml_lf.DamlLf

object ParticipantStateConversion {

  def V1ToV2Write(ws: v1.WriteService): v2.WriteService = new v2.WriteService {
    override def submitTransaction(
        submitterInfo: SubmitterInfo,
        transactionMeta: TransactionMeta,
        transaction: SubmittedTransaction): CompletionStage[SubmissionResult] = {
      ws.submitTransaction(
          v1.SubmitterInfo(
            submitterInfo.submitter,
            submitterInfo.applicationId,
            submitterInfo.commandId,
            Time.Timestamp.assertFromInstant(submitterInfo.maxRecordTime)),
          v1.TransactionMeta(
            Time.Timestamp.assertFromInstant(transactionMeta.ledgerEffectiveTime),
            transactionMeta.workflowId),
          transaction
        )
        .thenApply(sr =>
          sr match {
            case v1.SubmissionResult.Acknowledged => v2.SubmissionResult.Acknowledged
            case v1.SubmissionResult.Overloaded => v2.SubmissionResult.Overloaded
            case v1.SubmissionResult.NotSupported => v2.SubmissionResult.NotSupported
        })
    }

    override def uploadPackages(
        payload: List[DamlLf.Archive],
        sourceDescription: Option[String]): CompletionStage[UploadPackagesResult] = {
      ws.uploadPackages(payload, sourceDescription)
        .thenApply(result =>
          result match {
            case v1.UploadPackagesResult.InvalidPackage(r) =>
              v2.UploadPackagesResult.InvalidPackage(r)
            case v1.UploadPackagesResult.Ok => v2.UploadPackagesResult.Ok
            case v1.UploadPackagesResult.ParticipantNotAuthorized =>
              v2.UploadPackagesResult.ParticipantNotAuthorized
            case v1.UploadPackagesResult.NotSupported =>
              v2.UploadPackagesResult.InvalidPackage("Not supported")
        })
    }

    override def allocateParty(
        hint: Option[String],
        displayName: Option[String]): CompletionStage[PartyAllocationResult] = {
      ws.allocateParty(hint, displayName)
        .thenApply(result =>
          result match {
            case v1.PartyAllocationResult.Ok(details) => v2.PartyAllocationResult.Ok(details)
            case v1.PartyAllocationResult.AlreadyExists => v2.PartyAllocationResult.AlreadyExists
            case v1.PartyAllocationResult.ParticipantNotAuthorized =>
              v2.PartyAllocationResult.ParticipantNotAuthorized
            case v1.PartyAllocationResult.InvalidName(name) =>
              v2.PartyAllocationResult.InvalidName(name)
            case v1.PartyAllocationResult.NotSupported =>
              v2.PartyAllocationResult.InvalidName("Not supported")
        })
    }
  }

  def V1ToV2Rread(rs: v1.ReadService): v2.ReadService = new ReadService {
    override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = {
      rs.getLedgerInitialConditions()
        .map(
          lc =>
            v2.LedgerInitialConditions(
              lc.ledgerId,
              configurationToV2(lc.config),
              lc.initialRecordTime
          ))
    }

    override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
      rs.stateUpdates(beginAfter.map(o => v1.Offset.assertFromString(o.toLedgerString)))
        .map {
          case (offset, update) =>
            offsetToV2(offset) -> (update match {

              case v1.Update.CommandRejected(submitterInfo, reason) =>
                v2.Update.CommandRejected(
                  submitterInfoToV2(submitterInfo),
                  rejectionReasonToV2(reason)
                )

              case v1.Update.Heartbeat(recordTime) =>
                v2.Update.Heartbeat(recordTime.toInstant)

              case v1.Update
                    .PartyAddedToParticipant(party, displayName, participantId, recordTime) =>
                v2.Update.PartyAddedToParticipant(party, displayName, participantId, recordTime)

              case v1.Update.ConfigurationChanged(newConfiguration) =>
                v2.Update.ConfigurationChanged(configurationToV2(newConfiguration))

              case v1.Update.PublicPackagesUploaded(
                  archives,
                  sourceDescription,
                  participantId,
                  recordTime) =>
                v2.Update.PublicPackagesUploaded(
                  archives,
                  sourceDescription.getOrElse(""),
                  participantId,
                  recordTime)

              case v1.Update.TransactionAccepted(
                  optSubmitterInfo,
                  transactionMeta,
                  transaction,
                  transactionId,
                  recordTime,
                  referencedContracts) =>
                v2.Update.TransactionAccepted(
                  optSubmitterInfo.map(submitterInfoToV2),
                  v2.TransactionMeta(
                    transactionMeta.ledgerEffectiveTime.toInstant,
                    transactionMeta.workflowId),
                  transaction,
                  transactionId,
                  recordTime.toInstant,
                  referencedContracts
                )
            })
        }
    }
  }

  def offsetToV2(offset: v1.Offset): v2.Offset = v2.Offset.assertFromString(offset.toLedgerString)

  def submitterInfoToV2(info: v1.SubmitterInfo): v2.SubmitterInfo = {
    v2.SubmitterInfo(
      info.submitter,
      info.applicationId,
      info.commandId,
      info.maxRecordTime.toInstant)
  }

  def configurationToV2(configuration: v1.Configuration): v2.Configuration =
    v2.Configuration(new TimeModel {
      import configuration.timeModel

      override def minTransactionLatency: Duration = timeModel.minTransactionLatency
      override def futureAcceptanceWindow: Duration = timeModel.futureAcceptanceWindow
      override def maxClockSkew: Duration = timeModel.maxClockSkew
      override def minTtl: Duration = timeModel.minTtl
      override def maxTtl: Duration = timeModel.maxTtl
    })

  def rejectionReasonToV2(rejection: v1.RejectionReason): v2.RejectionReason = rejection match {
    case v1.RejectionReason.Disputed(reason) => v2.RejectionReason.Disputed(reason)
    case v1.RejectionReason.DuplicateCommand => v2.RejectionReason.DuplicateCommand
    case v1.RejectionReason.Inconsistent => v2.RejectionReason.Inconsistent
    case v1.RejectionReason.MaximumRecordTimeExceeded =>
      v2.RejectionReason.MaximumRecordTimeExceeded
    case v1.RejectionReason.PartyNotKnownOnLedger => v2.RejectionReason.PartyNotKnownOnLedger
    case v1.RejectionReason.SubmitterCannotActViaParticipant(details) =>
      v2.RejectionReason.SubmitterCannotActViaParticipant(details)
    case v1.RejectionReason.ResourcesExhausted => v2.RejectionReason.ResourcesExhausted
  }

}
