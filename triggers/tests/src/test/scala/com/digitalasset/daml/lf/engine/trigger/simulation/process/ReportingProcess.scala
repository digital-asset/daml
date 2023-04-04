// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation
package process

import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.google.rpc.status.{Status => GrpcStatus}

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.collection.immutable.TreeMap

private[simulation] object ReportingProcess {
  // Changes we need to make to a triggers in-memory ACS in order to match the ledger's ACS source of truth
  final case class ACSTemplateDiff(additions: Int, deletions: Int, common: Int)
  final case class ACSDiff(diff: Map[Identifier, ACSTemplateDiff])

  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class TriggerUpdate(
      triggerId: UUID,
      triggerType: Identifier,
      submissions: Seq[SubmitRequest],
      metrics: TriggerRuleMetrics.RuleMetrics,
      acsView: TreeMap[String, Identifier],
      percentageHeapUsed: Double,
      gcTime: Long,
      gcCount: Long,
      completionStatus: Option[GrpcStatus],
  ) extends Message
  // Used by LedgerProcess
  private[process] final case class TriggerACSDiff(
      reportingId: UUID,
      triggerId: UUID,
      diff: ACSDiff,
  ) extends Message

  def create(
      ledger: ActorRef[LedgerProcess.LedgerManagement]
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.setup { _ =>
      val triggerDataFile = Files.newOutputStream(Paths.get(config.triggerDataFile))
      val triggerDataFileCsvHeader =
        "reporting-id,trigger-name,trigger-id,submissions,evaluation-steps,evaluation-get-times,rule-evaluation-time,active-contracts,pending-contracts,in-flight-commands,percentage-heap-used,gc-time,gc-count,completion-status-code\n"
      val acsDataFile = Files.newOutputStream(Paths.get(config.acsDataFile))
      val acsDataFileCsvHeader =
        "reporting-id,trigger-id,template-id,contract-additions,contract-deletions\n"
      triggerDataFile.write(triggerDataFileCsvHeader.getBytes)
      acsDataFile.write(acsDataFileCsvHeader.getBytes)

      Behaviors
        .receiveMessage[Message] {
          case TriggerUpdate(
                triggerId,
                triggerType,
                submissions,
                metrics,
                triggerACSView,
                percentageHeapUsed,
                gcTime,
                gcCount,
                completionStatus,
              ) =>
            val reportingId = UUID.randomUUID()
            val csvData: String =
              s"$reportingId,$triggerId,$triggerType,${submissions.size},${metrics.evaluation.steps},${metrics.evaluation.getTimes},${metrics.evaluation.ruleEvaluation.toNanos},${metrics.endState.acs.activeContracts},${metrics.endState.acs.pendingContracts},${metrics.endState.inFlight.commands},$percentageHeapUsed,$gcTime,$gcCount,${completionStatus
                  .fold("")(_.code.toString)}\n"
            triggerDataFile.write(csvData.getBytes)
            ledger ! LedgerProcess.GetTriggerACSDiff(reportingId, triggerId, triggerACSView)
            Behaviors.same

          case TriggerACSDiff(reportingId, triggerId, acs) =>
            acs.diff.foreach { case (templateId, contracts) =>
              val csvData: String =
                s"$reportingId,$triggerId,$templateId,${contracts.additions},${contracts.deletions}\n"
              acsDataFile.write(csvData.getBytes)
            }
            Behaviors.same
        }
        .receiveSignal { case (_, PostStop) =>
          triggerDataFile.flush()
          acsDataFile.flush()
          triggerDataFile.close()
          acsDataFile.close()
          Behaviors.same
        }
    }
  }
}
