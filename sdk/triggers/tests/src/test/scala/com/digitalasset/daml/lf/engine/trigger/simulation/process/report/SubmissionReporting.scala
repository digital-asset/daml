// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package report

import org.apache.pekko.actor.typed.{Behavior, PostStop}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.lf.data.Ref
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.google.rpc.status.{Status => GrpcStatus}

import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable

private[simulation] object SubmissionReporting {
  sealed abstract class Message extends Product with Serializable
  // Used by LedgerProcess
  private[process] final case class SubmissionUpdate(
      timestamp: Long,
      commandId: String,
      reportId: UUID,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
      request: SubmitRequest,
  ) extends Message
  // Used by LedgerProcess
  private[process] final case class CompletionUpdate(
      timestamp: Long,
      commandId: String,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
      completion: Completion,
  ) extends Message
  private final case class ReportCompletedSubmission(
      submissionTimestamp: Long,
      completionTimestamp: Long,
      commandId: String,
      reportId: UUID,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
      completionStatus: GrpcStatus,
  ) extends Message

  def create()(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.setup { context =>
      val receivedSubmissions = mutable.Map.empty[UUID, mutable.Map[String, SubmissionUpdate]]
      val receivedCompletions = mutable.Map.empty[UUID, mutable.Map[String, CompletionUpdate]]
      val submissionDataFile = Files.newOutputStream(config.submissionDataFile)
      val submissionDataFileCsvHeader = Seq(
        "timestamp",
        "command-id",
        "report-id",
        "trigger-id",
        "trigger-def-ref",
        "submission-duration",
        "completion-status-code",
      ).mkString("", ",", "\n")
      submissionDataFile.write(submissionDataFileCsvHeader.getBytes)

      Behaviors
        .receiveMessage[Message] {
          case update @ SubmissionUpdate(_, commandId, reportId, triggerId, triggerDefRef, _) =>
            if (!receivedSubmissions.contains(triggerId)) {
              receivedSubmissions.update(triggerId, mutable.Map.empty[String, SubmissionUpdate])
            }
            if (!receivedCompletions.contains(triggerId)) {
              receivedCompletions.update(triggerId, mutable.Map.empty[String, CompletionUpdate])
            }
            if (receivedSubmissions(triggerId).contains(commandId)) {
              context.log.warn(s"Dropping duplicate submission update for command ID $commandId")
            } else {
              receivedSubmissions(triggerId).update(commandId, update)
            }
            if (receivedCompletions(triggerId).contains(commandId)) {
              val submission = receivedSubmissions(triggerId).remove(commandId).get
              val completion = receivedCompletions(triggerId).remove(commandId).get
              context.self ! ReportCompletedSubmission(
                submission.timestamp,
                completion.timestamp,
                commandId,
                reportId,
                triggerId,
                triggerDefRef,
                completion.completion.getStatus,
              )
            }
            Behaviors.same

          case update @ CompletionUpdate(_, commandId, triggerId, triggerDefRef, _) =>
            if (!receivedSubmissions.contains(triggerId)) {
              receivedSubmissions.update(triggerId, mutable.Map.empty[String, SubmissionUpdate])
            }
            if (!receivedCompletions.contains(triggerId)) {
              receivedCompletions.update(triggerId, mutable.Map.empty[String, CompletionUpdate])
            }
            if (receivedCompletions(triggerId).contains(commandId)) {
              context.log.warn(s"Dropping duplicate completion update for command ID $commandId")
            } else {
              receivedCompletions(triggerId).update(commandId, update)
            }
            if (receivedSubmissions(triggerId).contains(commandId)) {
              val submission = receivedSubmissions(triggerId).remove(commandId).get
              val completion = receivedCompletions(triggerId).remove(commandId).get
              context.self ! ReportCompletedSubmission(
                submission.timestamp,
                completion.timestamp,
                commandId,
                submission.reportId,
                triggerId,
                triggerDefRef,
                completion.completion.getStatus,
              )
            }
            Behaviors.same

          case ReportCompletedSubmission(
                submissionTimestamp,
                completionTimestamp,
                commandId,
                reportId,
                triggerId,
                triggerDefRef,
                completionStatus,
              ) =>
            val csvData: String = Seq[Any](
              submissionTimestamp,
              commandId,
              reportId,
              triggerId,
              triggerDefRef,
              completionTimestamp - submissionTimestamp,
              extractStatusMessage(completionStatus),
            ).mkString("", ",", "\n")
            submissionDataFile.write(csvData.getBytes)
            Behaviors.same
        }
        .receiveSignal { case (_, PostStop) =>
          // Ensure all incomplete submissions are saved
          for {
            (_, triggerSubmissions) <- receivedSubmissions
            (commandId, submission) <- triggerSubmissions
          } yield {
            val csvData: String = Seq[Any](
              submission.timestamp,
              commandId,
              submission.reportId,
              submission.triggerId,
              submission.triggerDefRef,
              "INCOMPLETE",
              "INCOMPLETE",
            ).mkString("", ",", "\n")
            submissionDataFile.write(csvData.getBytes)
          }
          submissionDataFile.flush()
          submissionDataFile.close()
          Behaviors.same
        }
    }
  }

  // We attempt to record just the completion summary prefix message (and not its detailed explanation)
  private[this] def extractStatusMessage(status: GrpcStatus): String = {
    val failureMessage = status.message.takeWhile('(' != _)

    if (failureMessage.nonEmpty) failureMessage else status.code.toString
  }
}
