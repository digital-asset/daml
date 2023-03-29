// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.value
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement.CompletionElement
import com.daml.ledger.sandbox.SandboxOnXForTest.ParticipantId
import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType
import com.daml.scalautil.Statement.discard
import org.scalacheck.Gen
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.TreeMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

abstract class TriggerMultiProcessSimulation
    extends AsyncWordSpec
    with SuiteResourceManagementAroundAll
    with AbstractTriggerTest {

  import TriggerMultiProcessSimulation._

  // FIXME: changed default value for test debugging
  protected implicit lazy val simulationConfig: TriggerSimulationConfig =
    TriggerSimulationConfig(simulationDuration = 30.seconds)

  protected implicit lazy val simulation: ActorSystem[Unit] =
    ActorSystem(triggerMultiProcessSimulationWithTimeout, "cat-and-food-simulation")

  override implicit lazy val materializer: Materializer = Materializer(simulation)

  override implicit lazy val executionContext: ExecutionContext = materializer.executionContext

  override protected val applicationId: ApplicationId = ApplicationId(
    "trigger-multi-process-simulation"
  )

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  "Multi process trigger simulation" in {
    for {
      _ <- simulation.whenTerminated
    } yield succeed
  }

  protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    Behaviors.receive { (context, _) =>
      context.log.info(s"Simulation timed out after: ${simulationConfig.simulationDuration}")
      Behaviors.stopped
    }
  }

  private[this] def triggerMultiProcessSimulationWithTimeout: Behavior[Unit] = {
    Behaviors.withTimers[Unit] { timer =>
      timer.startSingleTimer((), simulationConfig.simulationDuration)

      Behaviors
        .supervise(triggerMultiProcessSimulation)
        .onFailure[Throwable](SupervisorStrategy.stop)
    }
  }

  protected def triggerProcessFactory(
      client: LedgerClient,
      ledger: ActorRef[LedgerProcess.LedgerManagement],
      name: String,
      actAs: Party,
  ): TriggerProcessFactory = {
    new TriggerProcessFactory(
      client,
      ledger,
      name,
      packageId,
      applicationId,
      compiledPackages,
      config.participants(ParticipantId).apiServer.timeProviderType,
      triggerRunnerConfiguration,
      actAs,
    )
  }
}

object TriggerMultiProcessSimulation {

  final case class TriggerSimulationConfig(
      simulationSetupTimeout: FiniteDuration = 30.seconds,
      simulationDuration: FiniteDuration = 5.minutes,
      ledgerSubmissionTimeout: FiniteDuration = 30.seconds,
      ledgerRegistrationTimeout: FiniteDuration = 30.seconds,
      externalLedgerInteractionPeriod: FiniteDuration = 5.seconds,
      triggerDataFile: String = "trigger-simulation-data.csv",
      acsDataFile: String = "trigger-simulation-acs-data.csv",
  )

  final case class TriggerSimulationFailure(cause: Throwable) extends Exception

  object TriggerSimulationFailure {
    def apply(reason: String): TriggerSimulationFailure =
      TriggerSimulationFailure(new RuntimeException(reason))
  }

  object LedgerProcess {
    sealed abstract class Message extends Product with Serializable
    // Used by TriggerProcess
    private[TriggerMultiProcessSimulation] final case class CommandSubmission(
        request: SubmitRequest
    ) extends Message

    sealed abstract class LedgerManagement extends Product with Serializable
    // Used by TriggerProcess
    private[TriggerMultiProcessSimulation] final case class LedgerRegistration(
        triggerId: Identifier,
        trigger: ActorRef[TriggerProcess.Message],
        actAs: Party,
        filter: TransactionFilter,
        replyTo: ActorRef[LedgerApi],
    ) extends LedgerManagement
    // Used by CreateContractProcess
    private[TriggerMultiProcessSimulation] final case class CreateContract(
        create: CreatedEvent,
        party: Party,
    ) extends LedgerManagement
    // Used by ArchiveContractProcess
    private[TriggerMultiProcessSimulation] final case class ArchiveContract(
        archive: ArchivedEvent,
        party: Party,
    ) extends LedgerManagement
    // Used by ReportingProcess
    private[TriggerMultiProcessSimulation] final case class GetTriggerACSDiff(
        reportingId: UUID,
        triggerId: Identifier,
        triggerACSView: Map[String, Identifier],
    ) extends LedgerManagement

    // Used by TriggerProcess
    private[TriggerMultiProcessSimulation] final case class LedgerApi(
        api: ActorRef[Message],
        report: ActorRef[ReportingProcess.Message],
    )

    def create(client: LedgerClient, testSuite: AbstractTriggerTest)(implicit
        materializer: Materializer,
        config: TriggerSimulationConfig,
    ): Behavior[LedgerManagement] = {
      implicit val ec: ExecutionContext = materializer.executionContext

      Behaviors.setup { context =>
        val report = context.spawn(ReportingProcess.create(context.self), "reporting")
        val api = context.spawn(run(client), "ledger-api")
        // Map[TriggerId, Map[ContractId, TemplateId]]
        val ledgerACSView: TrieMap[Identifier, TrieMap[String, Identifier]] = TrieMap.empty

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
                  testSuite.create(client, party.unwrap, createCommand),
                  config.externalLedgerInteractionPeriod,
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
                  testSuite.archive(client, party.unwrap, event.getTemplateId, event.contractId),
                  config.externalLedgerInteractionPeriod,
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

  object AllowFiltering {
    def apply(
        filter: TriggerMsg.Transaction => Boolean
    )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
      Behaviors.setup { _ =>
        Behaviors.receiveMessage {
          case TriggerProcess.MessageWrapper(msg: TriggerMsg.Transaction) if !filter(msg) =>
            Behaviors.same

          case msg =>
            consumer ! msg
            Behaviors.same
        }
      }
    }
  }

  object AllowHeartbeats {
    def apply(
        duration: FiniteDuration
    )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
      Behaviors.withTimers[TriggerProcess.Message] { timer =>
        timer.startTimerAtFixedRate(TriggerProcess.MessageWrapper(TriggerMsg.Heartbeat), duration)

        Behaviors.receiveMessage { msg =>
          consumer ! msg
          Behaviors.same
        }
      }
    }
  }

  private object ReportingProcess {
    // Changes we need to make to a triggers in-memory ACS in order to match the ledger's ACS source of truth
    final case class ACSTemplateDiff(additions: Int, deletions: Int, common: Int)
    final case class ACSDiff(diff: Map[Identifier, ACSTemplateDiff])

    sealed abstract class Message extends Product with Serializable
    // Used by TriggerProcess
    private[TriggerMultiProcessSimulation] final case class TriggerUpdate(
        triggerId: Identifier,
        submissions: Seq[SubmitRequest],
        metrics: TriggerRuleMetrics.RuleMetrics,
        acsView: TreeMap[String, Identifier],
        percentageHeapUsed: Double,
        gcTime: Long,
        gcCount: Long,
    ) extends Message
    // Used by LedgerProcess
    private[TriggerMultiProcessSimulation] final case class TriggerACSDiff(
        reportingId: UUID,
        triggerId: Identifier,
        diff: ACSDiff,
    ) extends Message

    def create(
        ledger: ActorRef[LedgerProcess.LedgerManagement]
    )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
      Behaviors.setup { _ =>
        val triggerDataFile = Files.newOutputStream(Paths.get(config.triggerDataFile))
        val triggerDataFileCsvHeader =
          "reporting-id,trigger-id,submissions,evaluation-steps,evaluation-get-times,rule-evaluation-time,active-contracts,pending-contracts,in-flight-commands,percentage-heap-used,gc-time,gc-count\n"
        val acsDataFile = Files.newOutputStream(Paths.get(config.acsDataFile))
        val acsDataFileCsvHeader =
          "reporting-id,trigger-id,template-id,contract-additions,contract-deletions\n"
        triggerDataFile.write(triggerDataFileCsvHeader.getBytes)
        acsDataFile.write(acsDataFileCsvHeader.getBytes)

        Behaviors
          .receiveMessage[Message] {
            case TriggerUpdate(
                  triggerId,
                  submissions,
                  metrics,
                  triggerACSView,
                  percentageHeapUsed,
                  gcTime,
                  gcCount,
                ) =>
              val reportingId = UUID.randomUUID()
              val csvData: String =
                s"$reportingId,$triggerId,${submissions.size},${metrics.evaluation.steps},${metrics.evaluation.getTimes},${metrics.evaluation.ruleEvaluation.toNanos},${metrics.endState.acs.activeContracts},${metrics.endState.acs.pendingContracts},${metrics.endState.inFlight.commands},$percentageHeapUsed,$gcTime,$gcCount\n"
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

  object TriggerProcess {
    sealed abstract class Message extends Product with Serializable
    // Used by LedgerProcess
    private[TriggerMultiProcessSimulation] final case class LedgerResponse(
        api: ActorRef[LedgerProcess.Message],
        report: ActorRef[ReportingProcess.Message],
    ) extends Message
    // Used by all processes
    private[TriggerMultiProcessSimulation] final case class MessageWrapper(msg: TriggerMsg)
        extends Message
  }

  final class TriggerProcessFactory private[TriggerMultiProcessSimulation] (
      client: LedgerClient,
      ledger: ActorRef[LedgerProcess.LedgerManagement],
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

    private[this] val triggerId = Identifier(packageId, QualifiedName.assertFromString(name))
    private[this] val triggerParties = TriggerParties(
      actAs = actAs,
      readAs = readAs,
    )
    private[this] val simulator = new TriggerRuleSimulationLib(
      compiledPackages,
      triggerId,
      triggerConfig,
      client,
      timeProviderType,
      applicationId,
      triggerParties,
    )
    private[this] val trigger = simulator.trigger.defn
    private[this] val memBean = ManagementFactory.getMemoryMXBean
    private[this] val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

    def create(userState: SValue, acs: Seq[CreatedEvent] = Seq.empty)(implicit
        config: TriggerSimulationConfig
    ): Behavior[Message] = {
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

      Behaviors.setup { context =>
        context.ask(
          ledger,
          LedgerProcess.LedgerRegistration(triggerId, context.self, actAs, transactionFilter, _),
        ) {
          case Success(LedgerProcess.LedgerApi(api, report)) =>
            LedgerResponse(api, report)

          case Failure(exn) =>
            throw TriggerSimulationFailure(exn)
        }

        Behaviors.receiveMessage {
          case LedgerResponse(api, report) =>
            run(api, report, startState)

          case msg: MessageWrapper =>
            context.log.error(
              s"Whilst waiting for a ledger response during trigger registration, we received an unexpected message: $msg"
            )
            Behaviors.stopped
        }
      }
    }

    private[this] def run(
        ledgerApi: ActorRef[LedgerProcess.Message],
        report: ActorRef[ReportingProcess.Message],
        state: SValue,
    )(implicit config: TriggerSimulationConfig): Behavior[Message] = {
      Behaviors.receive {
        case (_, MessageWrapper(msg)) =>
          val (submissions, metrics, nextState) = Await.result(
            simulator.updateStateLambda(state, msg),
            triggerConfig.hardLimit.ruleEvaluationTimeout,
          )
          val acs = Runner
            .getActiveContracts(state, trigger.level, trigger.version)
            .getOrElse(
              throw TriggerSimulationFailure(
                s"Failed to extract active contracts from the internal state for trigger: $triggerId"
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

          submissions.foreach { request =>
            ledgerApi ! LedgerProcess.CommandSubmission(request)
          }
          report ! ReportingProcess.TriggerUpdate(
            triggerId,
            submissions,
            metrics,
            triggerACSView,
            percentageHeapUsed,
            gcTime,
            gcCount,
          )

          run(ledgerApi, report, state = nextState)

        case (context, response: LedgerResponse) =>
          context.log.error(
            s"Having registered with the ledger, we received an unexpected registration response: $response"
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

  object CreateContractProcess {
    sealed abstract class Message extends Product with Serializable
    private case object CreateContract extends Message

    def create(
        gen: Gen[CreatedEvent],
        ledger: ActorRef[LedgerProcess.LedgerManagement],
        party: Party,
    )(implicit
        config: TriggerSimulationConfig
    ): Behavior[Message] = {
      Behaviors.withTimers[Message] { timer =>
        timer.startTimerAtFixedRate(CreateContract, config.externalLedgerInteractionPeriod)

        Behaviors.receiveMessage { case CreateContract =>
          gen.sample.foreach { event =>
            ledger ! LedgerProcess.CreateContract(event, party)
          }
          Behaviors.same
        }
      }
    }
  }

  object ArchiveContractProcess {
    sealed abstract class Message extends Product with Serializable
    private case object ArchiveContract extends Message

    def create(
        gen: Gen[ArchivedEvent],
        ledger: ActorRef[LedgerProcess.LedgerManagement],
        party: Party,
    )(implicit
        config: TriggerSimulationConfig
    ): Behavior[Message] = {
      Behaviors.withTimers[Message] { timer =>
        timer.startTimerAtFixedRate(ArchiveContract, config.externalLedgerInteractionPeriod)

        Behaviors.receiveMessage { case ArchiveContract =>
          gen.sample.foreach { event =>
            ledger ! LedgerProcess.ArchiveContract(event, party)
          }
          Behaviors.same
        }
      }
    }
  }
}
