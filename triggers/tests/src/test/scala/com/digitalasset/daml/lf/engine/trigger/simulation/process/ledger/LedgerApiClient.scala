// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.LedgerClient
import com.daml.lf.engine.trigger.TriggerMsg
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.scalautil.Statement.discard
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException

import scala.concurrent.Await
import scala.util.control.NonFatal

object LedgerApiClient {
  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class CommandSubmission(
      request: SubmitRequest,
      replyTo: ActorRef[TriggerProcess.Message],
  ) extends Message

  def create(
      client: LedgerClient
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.receive { case (context, CommandSubmission(request, replyTo)) =>
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
          replyTo ! TriggerProcess.MessageWrapper(
            TriggerMsg.Completion(
              Completion(
                request.getCommands.commandId,
                Some(Status(cause.getStatus.getCode.value(), cause.getStatus.getDescription)),
              )
            )
          )

        case NonFatal(reason) =>
          context.log.warn(
            s"Ledger API encountered a command submission failure for $request - ignoring this failure",
            reason,
          )
      }
      Behaviors.same
    }
  }
}
