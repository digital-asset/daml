// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.scalautil.Statement.discard

import scala.concurrent.{Await, ExecutionContext, TimeoutException}
import scala.util.control.NonFatal

final class LedgerExternalAction(client: LedgerClient)(implicit
    materializer: Materializer,
    config: TriggerSimulationConfig,
    applicationId: ApplicationId,
) {

  import LedgerExternalAction._

  def create(): Behavior[Message] = {
    implicit val ec: ExecutionContext = materializer.executionContext

    Behaviors.receive {
      case (context, CreateContract(event, party)) =>
        val createCommand = CreateCommand(event.templateId, event.createArguments)
        try {
          discard(
            Await.result(
              AbstractTriggerTest.create(client, party, createCommand),
              config.ledgerWorkloadTimeout,
            )
          )
        } catch {
          case exn: TimeoutException =>
            throw exn

          case NonFatal(reason) =>
            context.log.warn(
              s"Ignoring create event submission failure for $event",
              reason,
            )
        }
        Behaviors.same

      case (context, ArchiveContract(event, party)) =>
        try {
          discard(
            Await.result(
              AbstractTriggerTest
                .archive(client, party, event.getTemplateId, event.contractId),
              config.ledgerWorkloadTimeout,
            )
          )
        } catch {
          case exn: TimeoutException =>
            throw exn

          case NonFatal(reason) =>
            context.log.warn(
              s"Ignoring archive event submission failure for $event",
              reason,
            )
        }
        Behaviors.same
    }
  }
}

object LedgerExternalAction {
  sealed abstract class Message extends Product with Serializable
  // Used by LedgerProcess
  final case class CreateContract(
      create: CreatedEvent,
      party: Party,
  ) extends Message
  // Used by LedgerProcess
  final case class ArchiveContract(
      archive: ArchivedEvent,
      party: Party,
  ) extends Message
}
