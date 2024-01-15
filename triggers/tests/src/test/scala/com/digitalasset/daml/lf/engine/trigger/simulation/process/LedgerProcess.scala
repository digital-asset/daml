// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.engine.trigger.simulation.ReportingProcess
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig

object LedgerProcess {
  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class TriggerRegistration(
      registration: LedgerRegistration.Registration
  ) extends Message
  // Used by ReportingProcess
  private[process] final case class GetTriggerACSDiff(
      request: LedgerRegistration.GetTriggerACSDiff
  ) extends Message
  // Used by (external) workload processes
  final case class ExternalAction(request: LedgerExternalAction.Message) extends Message

  def create(client: LedgerClient)(implicit
      materializer: Materializer,
      config: TriggerSimulationConfig,
      applicationId: Option[Ref.ApplicationId],
  ): Behavior[Message] = {
    Behaviors.setup { context =>
      val report = context.spawn(ReportingProcess.create(context.self), "reporting")
      val ledgerApi = context.spawn(LedgerApiClient.create(client, report), "ledger-api")
      val ledgerExternal =
        context.spawn(new LedgerExternalAction(client).create(), "ledger-external-action")
      val triggerRegistration =
        context.spawn(
          new LedgerRegistration(client).create(ledgerApi, report),
          "trigger-registration",
        )

      context.watch(report)
      context.watch(ledgerApi)
      context.watch(ledgerExternal)
      context.watch(triggerRegistration)

      Behaviors.receiveMessage {
        case msg @ TriggerRegistration(registration) =>
          context.log.debug(s"actor [${context.self}] - received message: $msg")
          triggerRegistration ! registration
          Behaviors.same

        case GetTriggerACSDiff(request) =>
          context.log.debug(s"actor [${context.self}] - received message: GetTriggerACSDiff(..)")
          triggerRegistration ! request
          Behaviors.same

        case msg @ ExternalAction(request) =>
          context.log.debug(s"actor [${context.self}] - received message: $msg")
          ledgerExternal ! request
          Behaviors.same
      }
    }
  }
}
