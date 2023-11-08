// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation
package process

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.client.LedgerClient
import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.{
  TriggerSimulationConfig,
  TriggerSimulationFailure,
}
import com.daml.lf.engine.trigger.simulation.process.ledger.{
  LedgerApiClient,
  LedgerProcess,
  LedgerRegistration,
}
import com.daml.lf.engine.trigger.simulation.process.report.{ACSReporting, MetricsReporting}
import com.daml.lf.engine.trigger.{
  Converter,
  Runner,
  TriggerMsg,
  TriggerParties,
  TriggerRunnerConfig,
}
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType

import java.lang.management.ManagementFactory
import java.util.UUID
import scala.collection.immutable.TreeMap
import scala.concurrent.Await
import scala.util.{Failure, Success}
import scalaz.syntax.tag._

import scala.jdk.CollectionConverters._

object TriggerProcess {
  sealed abstract class Message extends Product with Serializable
  // Used by LedgerProcess
  private[process] final case class LedgerResponse(
      api: ActorRef[LedgerApiClient.Message],
      report: ActorRef[ReportingProcess.Message],
  ) extends Message
  // Used by all processes
  private[process] final case class MessageWrapper(msg: TriggerMsg) extends Message
  // Used by TriggerInitialize wrapper process
  private[process] final case class Initialize(userState: SValue) extends Message
}

final class TriggerProcessFactory private[simulation] (
    client: LedgerClient,
    ledger: ActorRef[LedgerProcess.Message],
    name: String,
    packageId: PackageId,
    applicationId: ApplicationId,
    compiledPackages: PureCompiledPackages,
    timeProviderType: TimeProviderType,
    triggerConfig: TriggerRunnerConfig,
    actAs: Party,
    readAs: Set[Party] = Set.empty,
)(implicit materializer: Materializer) {

  import TriggerProcess._

  private[this] val triggerDefRef =
    Ref.DefinitionRef(packageId, QualifiedName.assertFromString(name))
  private[this] val triggerParties = TriggerParties(
    actAs = actAs,
    readAs = readAs,
  )
  private[this] val simulator = new TriggerRuleSimulationLib(
    compiledPackages,
    triggerDefRef,
    triggerConfig,
    client,
    timeProviderType,
    applicationId,
    triggerParties,
  )
  private[this] val trigger = simulator.trigger.defn
  private[this] val memBean = ManagementFactory.getMemoryMXBean
  private[this] val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

  def create(acs: Seq[CreatedEvent])(implicit
      config: TriggerSimulationConfig
  ): Behavior[Message] = {
    Behaviors.receive {
      case (_, Initialize(userState)) =>
        create(userState, acs)

      case (context, msg) =>
        context.log.error(s"Trigger process received $msg without receiving an Initialize message")
        Behaviors.stopped
    }
  }

  def create(userState: SValue, acs: Seq[CreatedEvent] = Seq.empty)(implicit
      config: TriggerSimulationConfig
  ): Behavior[Message] = {
    val triggerId = UUID.randomUUID()
    val converter = new Converter(compiledPackages, trigger)
    val startState = converter
      .fromTriggerUpdateState(
        acs,
        userState,
        parties = triggerParties,
        triggerConfig = triggerConfig,
      )
    val transactionFilter =
      TransactionFilter(
        triggerParties.readers.map(p => (p.unwrap, simulator.trigger.filters)).toMap
      )

    implicit val ledgerResponseTimeout: Timeout = Timeout(config.ledgerRegistrationTimeout)

    Behaviors.logMessages {
      Behaviors.setup { context =>
        context.ask(
          ledger,
          (ref: ActorRef[LedgerRegistration.LedgerApi]) =>
            LedgerProcess.TriggerRegistration(
              LedgerRegistration
                .Registration(triggerId, triggerDefRef, context.self, actAs, transactionFilter, ref)
            ),
        ) {
          case Success(LedgerRegistration.LedgerApi(api, report)) =>
            LedgerResponse(api, report)

          case Failure(exn) =>
            throw TriggerSimulationFailure(exn)
        }

        Behaviors.receiveMessage {
          case LedgerResponse(api, report) =>
            run(triggerId, api, report, startState)

          case msg =>
            context.log.error(
              s"Whilst waiting for a ledger response during trigger registration, we received an unexpected message: $msg"
            )
            Behaviors.stopped
        }
      }
    }
  }

  private[this] def run(
      triggerId: UUID,
      ledgerApi: ActorRef[LedgerApiClient.Message],
      report: ActorRef[ReportingProcess.Message],
      state: SValue,
  )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
    Behaviors.receive {
      case (context, MessageWrapper(msg)) =>
        val timestamp = System.currentTimeMillis()
        val (submissions, metrics, nextState) = Await.result(
          simulator.updateStateLambda(state, msg),
          triggerConfig.hardLimit.ruleEvaluationTimeout,
        )
        val acs = Runner
          .getActiveContracts(state, trigger.level, trigger.version)
          .getOrElse(
            throw TriggerSimulationFailure(
              s"Failed to extract active contracts from the internal state for trigger: $triggerDefRef"
            )
          )
        val triggerACSView: TreeMap[String, Identifier] = acs.flatMap { case (_, smap) =>
          smap.map { case (anyContractId, anyTemplate) =>
            (toContractId(anyContractId), toTemplateId(anyTemplate))
          }
        }
        val percentageHeapUsed =
          memBean.getHeapMemoryUsage.getUsed / memBean.getHeapMemoryUsage.getMax.toDouble
        val gcTime = gcBeans.asScala.map(_.getCollectionTime).sum
        val gcCount = gcBeans.asScala.map(_.getCollectionCount).sum
        val completionStatus = msg match {
          case TriggerMsg.Completion(completion) =>
            Some(completion.getStatus)
          case _ =>
            None
        }
        val reportId = UUID.randomUUID()

        submissions.foreach { request =>
          ledgerApi ! LedgerApiClient.CommandSubmission(
            request,
            context.self,
            timestamp,
            reportId,
            triggerId,
            triggerDefRef,
          )
        }
        report ! ReportingProcess.MetricsUpdate(
          MetricsReporting.TriggerMetricsUpdate(
            timestamp,
            reportId,
            triggerId,
            triggerDefRef,
            submissions,
            metrics,
            percentageHeapUsed,
            gcTime,
            gcCount,
            completionStatus,
          )
        )
        report ! ReportingProcess.ACSUpdate(
          ACSReporting.TriggerACSUpdate(
            timestamp,
            reportId,
            triggerId,
            triggerDefRef,
            triggerACSView,
          )
        )

        run(triggerId, ledgerApi, report, state = nextState)

      case (context, msg) =>
        context.log.error(
          s"Having registered with the ledger, we received an unexpected message: $msg"
        )
        Behaviors.stopped
    }
  }

  private[this] def toContractId(v: SValue): String = {
    Converter
      .toAnyContractId(v)
      .getOrElse(
        throw TriggerSimulationFailure(s"Failed to convert contract ID given SValue: $v")
      )
      .contractId
      .coid
  }

  private[this] def toTemplateId(v: SValue): Identifier = {
    Converter
      .toAnyTemplate(v)
      .getOrElse(
        throw TriggerSimulationFailure(s"Failed to convert template ID given SValue: $v")
      )
      .ty
  }
}
