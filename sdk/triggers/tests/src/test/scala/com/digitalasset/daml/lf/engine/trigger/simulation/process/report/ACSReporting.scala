// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package report

import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.process.ledger.{LedgerProcess, LedgerRegistration}

import java.nio.file.Files
import java.util.UUID
import scala.collection.immutable.TreeMap

private[simulation] object ACSReporting {
  // Changes we need to make to a triggers in-memory ACS in order to match the ledger's ACS source of truth
  final case class ACSTemplateDiff(additions: Int, deletions: Int, common: Int)
  final case class ACSDiff(diff: Map[Identifier, ACSTemplateDiff])

  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class TriggerACSUpdate(
      timestamp: Long,
      reportingId: UUID,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
      acsView: TreeMap[String, Identifier],
  ) extends Message
  // Used by LedgerProcess
  private[process] final case class TriggerACSDiff(
      timestamp: Long,
      reportingId: UUID,
      triggerId: UUID,
      triggerDefRef: Ref.DefinitionRef,
      diff: ACSDiff,
  ) extends Message

  def create(
      ledger: ActorRef[LedgerProcess.Message]
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.setup { _ =>
      val acsDataFile = Files.newOutputStream(config.acsDataFile)
      val acsDataFileCsvHeader =
        Seq(
          "timestamp",
          "reporting-id",
          "trigger-id",
          "trigger-def-ref",
          "template-id",
          "contract-additions",
          "contract-deletions",
        )
          .mkString("", ",", "\n")
      acsDataFile.write(acsDataFileCsvHeader.getBytes)

      Behaviors
        .receiveMessage[Message] {
          case TriggerACSUpdate(
                timestamp,
                reportingId,
                triggerId,
                triggerDefRef,
                triggerACSView,
              ) =>
            ledger ! LedgerProcess.GetTriggerACSDiff(
              LedgerRegistration
                .GetTriggerACSDiff(timestamp, reportingId, triggerId, triggerDefRef, triggerACSView)
            )
            Behaviors.same

          case TriggerACSDiff(timestamp, reportingId, triggerId, triggerDefRef, acs) =>
            acs.diff.foreach { case (templateId, contracts) =>
              val csvData: String =
                Seq[Any](
                  timestamp,
                  reportingId,
                  triggerId,
                  triggerDefRef,
                  templateId,
                  contracts.additions,
                  contracts.deletions,
                )
                  .mkString("", ",", "\n")
              acsDataFile.write(csvData.getBytes)
            }
            Behaviors.same
        }
        .receiveSignal { case (_, PostStop) =>
          acsDataFile.flush()
          acsDataFile.close()
          Behaviors.same
        }
    }
  }
}
