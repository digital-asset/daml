// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.value
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement.CompletionElement
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.engine.trigger.simulation.ReportingProcess
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.{
  TriggerSimulationConfig,
  TriggerSimulationFailure,
}
import com.daml.lf.engine.trigger.simulation.process.report.ACSReporting
import com.daml.lf.engine.trigger.{Converter, TriggerMsg}
import scalaz.syntax.tag._

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.TreeMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

final class LedgerRegistration(client: LedgerClient)(implicit
    materializer: Materializer,
    config: TriggerSimulationConfig,
) {

  import LedgerRegistration._

  def create(
      consumer: ActorRef[LedgerApiClient.Message],
      report: ActorRef[ReportingProcess.Message],
  ): Behavior[Message] = {
    implicit val ec: ExecutionContext = materializer.executionContext

    Behaviors.setup { context =>
      // Map[TriggerId, Map[ContractId, TemplateId]]
      val ledgerACSView: TrieMap[UUID, TrieMap[String, Identifier]] = TrieMap.empty

      Behaviors.receiveMessage {
        case Registration(triggerId, trigger, actAs, filter, replyTo)
            if !ledgerACSView.contains(triggerId) =>
          val offset =
            Await.result(getLedgerOffset(client, filter), config.simulationSetupTimeout)

          ledgerACSView += (triggerId -> TrieMap.empty)
          client.transactionClient
            .getTransactions(offset, None, filter)
            .runForeach { transaction =>
              transaction.events.foreach {
                case Event(Event.Event.Created(create)) =>
                  ledgerACSView(triggerId) += (create.contractId -> assertIdentifier(
                    create.getTemplateId
                  ))

                case Event(Event.Event.Archived(archive)) =>
                  ledgerACSView(triggerId) -= archive.contractId

                case Event(_) =>
                // Nothing to do
              }
              trigger ! TriggerProcess.MessageWrapper(TriggerMsg.Transaction(transaction))
            }
            .onComplete {
              case Failure(exn) =>
                throw exn

              case Success(_) =>
              // Do nothing
            }
          client.commandClient
            .completionSource(Seq(actAs.unwrap), offset)
            .collect { case CompletionElement(completion, _) =>
              trigger ! TriggerProcess.MessageWrapper(TriggerMsg.Completion(completion))
            }
            .run()
            .onComplete {
              case Failure(exn) =>
                throw exn

              case Success(_) =>
              // Do nothing
            }
          replyTo ! LedgerApi(consumer, report)
          Behaviors.same

        case Registration(triggerId, _, _, _, _) =>
          context.log.error(
            s"Following trigger registration, received another LedgerRegistration message for trigger: $triggerId"
          )
          Behaviors.stopped

        case APIMessage(triggerId, msg) if ledgerACSView.contains(triggerId) =>
          consumer ! msg
          Behaviors.same

        case APIMessage(triggerId, _) =>
          context.log.error(
            s"Received an API message from an unregistered trigger: $triggerId"
          )
          Behaviors.stopped

        case GetTriggerACSDiff(reportId, triggerId, triggerACSView) =>
          val diff = LedgerACSDiff(
            triggerACSView,
            ledgerACSView.getOrElse(triggerId, TrieMap.empty),
          )
          report ! ReportingProcess.ACSUpdate(
            ACSReporting.TriggerACSDiff(reportId, triggerId, diff)
          )
          Behaviors.same
      }
    }
  }

  private def getLedgerOffset(client: LedgerClient, filter: TransactionFilter)(implicit
      materializer: Materializer
  ): Future[LedgerOffset] = {
    implicit val ec: ExecutionContext = materializer.executionContext

    for {
      response <- client.activeContractSetClient
        .getActiveContracts(filter)
        .runWith(Sink.last)
      offset = LedgerOffset().withAbsolute(response.offset)
    } yield offset
  }

  private def assertIdentifier(identifier: value.Identifier): Identifier = {
    Converter
      .fromIdentifier(identifier)
      .getOrElse(throw TriggerSimulationFailure(s"Failed to convert Identifier for: $identifier"))
  }
}

object LedgerRegistration {
  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess (via LedgerProcess)
  private[process] final case class Registration(
      triggerId: UUID,
      trigger: ActorRef[TriggerProcess.Message],
      actAs: Party,
      filter: TransactionFilter,
      replyTo: ActorRef[LedgerApi],
  ) extends Message
  final case class APIMessage(triggerId: UUID, message: LedgerApiClient.Message) extends Message
  final case class GetTriggerACSDiff(
      reportID: UUID,
      triggerId: UUID,
      triggerACSView: TreeMap[String, Identifier],
  ) extends Message

  // Used by TriggerProcess
  private[process] final case class LedgerApi(
      api: ActorRef[LedgerApiClient.Message],
      report: ActorRef[ReportingProcess.Message],
  )
}
