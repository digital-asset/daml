// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package report

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.lf.data.Ref
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.TriggerRuleMetrics
import com.google.rpc.status.{Status => GrpcStatus}

import java.nio.file.Files
import java.util.UUID

private[simulation] object MetricsReporting {
  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class TriggerMetricsUpdate(
      timestamp: Long,
      reportingId: UUID,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
      submissions: Seq[SubmitRequest],
      metrics: TriggerRuleMetrics.RuleMetrics,
      percentageHeapUsed: Double,
      gcTime: Long,
      gcCount: Long,
      completionStatus: Option[GrpcStatus],
  ) extends Message

  def create()(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.setup { _ =>
      val triggerDataFile = Files.newOutputStream(config.triggerDataFile)
      val triggerDataFileCsvHeader = Seq(
        "timestamp",
        "reporting-id",
        "trigger-id",
        "trigger-def-ref",
        "submissions",
        "evaluation-steps",
        "evaluation-get-times",
        "rule-evaluation-time",
        "active-contracts",
        "pending-contracts",
        "in-flight-commands",
        "percentage-heap-used",
        "gc-time",
        "gc-count",
        "completion-status-code",
      ).mkString("", ",", "\n")
      triggerDataFile.write(triggerDataFileCsvHeader.getBytes)

      Behaviors
        .receiveMessage[Message] {
          case TriggerMetricsUpdate(
                timestamp,
                reportingId,
                triggerId,
                triggerDefRef,
                submissions,
                metrics,
                percentageHeapUsed,
                gcTime,
                gcCount,
                completionStatus,
              ) =>
            val csvData: String = Seq[Any](
              timestamp,
              reportingId,
              triggerId,
              triggerDefRef,
              submissions.size,
              metrics.evaluation.steps,
              metrics.evaluation.getTimes,
              metrics.evaluation.ruleEvaluation.toNanos,
              metrics.endState.acs.activeContracts,
              metrics.endState.acs.pendingContracts,
              metrics.endState.inFlight.commands,
              percentageHeapUsed,
              gcTime,
              gcCount,
              completionStatus.fold("")(extractStatusMessage),
            ).mkString("", ",", "\n")
            triggerDataFile.write(csvData.getBytes)
            Behaviors.same
        }
        .receiveSignal { case (_, PostStop) =>
          triggerDataFile.flush()
          triggerDataFile.close()
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
