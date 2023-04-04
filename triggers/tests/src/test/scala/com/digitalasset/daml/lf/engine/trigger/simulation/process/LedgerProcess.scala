// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation
package process

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.value
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement.CompletionElement
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.engine.trigger.{Converter, TriggerMsg}
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.{
  TriggerSimulationConfig,
  TriggerSimulationFailure,
}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.scalautil.Statement.discard

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal
import scalaz.syntax.tag._

object LedgerProcess {
  sealed abstract class Message extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class CommandSubmission(
      request: SubmitRequest
  ) extends Message

  sealed abstract class LedgerManagement extends Product with Serializable
  // Used by TriggerProcess
  private[process] final case class LedgerRegistration(
      triggerId: UUID,
      trigger: ActorRef[TriggerProcess.Message],
      actAs: Party,
      filter: TransactionFilter,
      replyTo: ActorRef[LedgerApi],
  ) extends LedgerManagement
  // Used by CreateContractProcess and external code
  final case class CreateContract(
      create: CreatedEvent,
      party: Party,
  ) extends LedgerManagement
  // Used by ArchiveContractProcess and external code
  final case class ArchiveContract(
      archive: ArchivedEvent,
      party: Party,
  ) extends LedgerManagement
  // Used by ReportingProcess
  private[process] final case class GetTriggerACSDiff(
      reportingId: UUID,
      triggerId: UUID,
      triggerACSView: Map[String, Identifier],
  ) extends LedgerManagement

  // Used by TriggerProcess
  private[process] final case class LedgerApi(
      api: ActorRef[Message],
      report: ActorRef[ReportingProcess.Message],
  )

  def create(client: LedgerClient)(implicit
      materializer: Materializer,
      config: TriggerSimulationConfig,
      applicationId: ApplicationId,
  ): Behavior[LedgerManagement] = {
    implicit val ec: ExecutionContext = materializer.executionContext

    Behaviors.setup { context =>
      val report = context.spawn(ReportingProcess.create(context.self), "reporting")
      val api = context.spawn(run(client), "ledger-api")
      // Map[TriggerId, Map[ContractId, TemplateId]]
      val ledgerACSView: TrieMap[UUID, TrieMap[String, Identifier]] = TrieMap.empty

      context.watch(report)
      context.watch(api)

      Behaviors.receiveMessage {
        case LedgerRegistration(triggerId, trigger, actAs, filter, replyTo)
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
          replyTo ! LedgerApi(api, report)
          Behaviors.same

        case msg: LedgerRegistration =>
          context.log.error(
            s"Following trigger registration, received another LedgerRegistration message for trigger: ${msg.triggerId}"
          )
          Behaviors.stopped

        case CreateContract(event, party) =>
          val createCommand = CreateCommand(event.templateId, event.createArguments)
          try {
            discard(
              Await.result(
                AbstractTriggerTest.create(client, party.unwrap, createCommand),
                config.ledgerWorkloadTimeout,
              )
            )
          } catch {
            case exn: TimeoutException =>
              throw exn

            case NonFatal(reason) =>
              context.log.warn(
                s"Ignoring create event submission failure: $event - reason: $reason"
              )
          }
          Behaviors.same

        case ArchiveContract(event, party) =>
          try {
            discard(
              Await.result(
                AbstractTriggerTest
                  .archive(client, party.unwrap, event.getTemplateId, event.contractId),
                config.ledgerWorkloadTimeout,
              )
            )
          } catch {
            case exn: TimeoutException =>
              throw exn

            case NonFatal(reason) =>
              context.log.warn(
                s"Ignoring archive event submission failure: $event - reason: $reason"
              )
          }
          Behaviors.same

        case GetTriggerACSDiff(reportingId, triggerId, triggerACSView) =>
          val common =
            ledgerACSView
              .getOrElse(triggerId, TrieMap.empty)
              .toSet
              .intersect(triggerACSView.toSet)
              .groupBy(_._2)
          val additions = ledgerACSView
            .getOrElse(triggerId, TrieMap.empty)
            .toSet
            .diff(triggerACSView.toSet)
            .groupBy(_._2)
          val deletions = triggerACSView.toSet
            .diff(ledgerACSView.getOrElse(triggerId, TrieMap.empty).toSet)
            .groupBy(_._2)
          val templates = triggerACSView.values.toSet ++ ledgerACSView
            .getOrElse(triggerId, TrieMap.empty)
            .values
            .toSet
          val diff = templates.map { templateId =>
            (
              templateId,
              ReportingProcess.ACSTemplateDiff(
                additions.getOrElse(templateId, Set.empty).size,
                deletions.getOrElse(templateId, Set.empty).size,
                common.getOrElse(templateId, Set.empty).size,
              ),
            )
          }
          report ! ReportingProcess.TriggerACSDiff(
            reportingId,
            triggerId,
            ReportingProcess.ACSDiff(diff.toMap),
          )
          Behaviors.same
      }
    }
  }

  private def run(
      client: LedgerClient
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.receiveMessage { case CommandSubmission(request) =>
      discard(
        Await.result(
          client.commandClient.submitSingleCommand(request),
          config.ledgerSubmissionTimeout,
        )
      )
      Behaviors.same
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
