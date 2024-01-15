// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.process.ledger.LedgerProcess
import com.daml.lf.engine.trigger.simulation.process.report.{
  ACSReporting,
  MetricsReporting,
  SubmissionReporting,
}

private[simulation] final case class ReportingProcess private (
    metrics: ActorRef[MetricsReporting.Message],
    acs: ActorRef[ACSReporting.Message],
)

private[simulation] object ReportingProcess {
  sealed abstract class Message extends Product with Serializable
  final case class MetricsUpdate(update: MetricsReporting.Message) extends Message
  final case class ACSUpdate(update: ACSReporting.Message) extends Message
  final case class SubmissionUpdate(update: SubmissionReporting.Message) extends Message

  def create(
      ledgerApi: ActorRef[LedgerProcess.Message]
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.setup { context =>
      val metrics = context.spawn(MetricsReporting.create(), "metrics-reporting")
      val acs = context.spawn(ACSReporting.create(ledgerApi), "acs-reporting")
      val submission = context.spawn(SubmissionReporting.create(), "submission-reporting")

      context.watch(metrics)
      context.watch(acs)

      Behaviors.receiveMessage {
        case MetricsUpdate(update) =>
          metrics ! update
          Behaviors.same

        case ACSUpdate(update) =>
          acs ! update
          Behaviors.same

        case SubmissionUpdate(update) =>
          submission ! update
          Behaviors.same
      }
    }
  }
}
