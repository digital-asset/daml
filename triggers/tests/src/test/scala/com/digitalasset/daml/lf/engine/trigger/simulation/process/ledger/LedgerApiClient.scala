// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.engine.trigger.TriggerMsg
import com.daml.lf.engine.trigger.simulation.ReportingProcess
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.process.report.SubmissionReporting
import com.daml.scalautil.Statement.discard
import com.google.rpc.code.Code.UNKNOWN
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException

import java.util.UUID
import scala.concurrent.Await
import scala.util.control.NonFatal

object LedgerApiClient {
  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class CommandSubmission(
      request: SubmitRequest,
      replyTo: ActorRef[TriggerProcess.Message],
      timestamp: Long,
      reportId: UUID,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
  ) extends Message

  def create(
      client: LedgerClient,
      report: ActorRef[ReportingProcess.Message],
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.receive {
      case (
            context,
            CommandSubmission(
              request,
              replyTo,
              submissionTimestamp,
              reportId,
              triggerId,
              triggerDefRef,
            ),
          ) =>
        val commandId = request.getCommands.commandId
        report ! ReportingProcess.SubmissionUpdate(
          SubmissionReporting.SubmissionUpdate(
            submissionTimestamp,
            commandId,
            reportId,
            triggerId,
            triggerDefRef,
            request,
          )
        )
        try {
          discard(
            Await.result(
              client.commandClient.submitSingleCommand(request),
              config.ledgerSubmissionTimeout,
            )
          )
        } catch {
          case cause: StatusRuntimeException if cause.getStatus.getCode != Code.UNAUTHENTICATED =>
            context.log.info(
              s"Ledger API encountered a command submission failure for $request - sending completion failure to $replyTo",
              cause,
            )
            val completionTimestamp = System.currentTimeMillis()
            val completion = Completion(
              request.getCommands.commandId,
              Some(Status(cause.getStatus.getCode.value(), cause.getStatus.getDescription)),
            )
            replyTo ! TriggerProcess.MessageWrapper(TriggerMsg.Completion(completion))
            report ! ReportingProcess.SubmissionUpdate(
              SubmissionReporting.CompletionUpdate(
                completionTimestamp,
                commandId,
                triggerId,
                triggerDefRef,
                completion,
              )
            )

          case cause: StatusRuntimeException =>
            context.log.warn(
              s"Ledger API encountered a command submission failure for $request - ignoring this failure",
              cause,
            )
            val completionTimestamp = System.currentTimeMillis()
            val completion = Completion(
              request.getCommands.commandId,
              Some(Status(cause.getStatus.getCode.value(), cause.getStatus.getDescription)),
            )
            report ! ReportingProcess.SubmissionUpdate(
              SubmissionReporting.CompletionUpdate(
                completionTimestamp,
                commandId,
                triggerId,
                triggerDefRef,
                completion,
              )
            )

          case NonFatal(reason) =>
            context.log.warn(
              s"Ledger API encountered a command submission failure for $request - ignoring this failure",
              reason,
            )
            val completionTimestamp = System.currentTimeMillis()
            val completion = Completion(
              request.getCommands.commandId,
              Some(Status(UNKNOWN.value, reason.getMessage)),
            )
            report ! ReportingProcess.SubmissionUpdate(
              SubmissionReporting.CompletionUpdate(
                completionTimestamp,
                commandId,
                triggerId,
                triggerDefRef,
                completion,
              )
            )
        }
        Behaviors.same
    }
  }
}
