// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.stream.{FlowShape, KillSwitches, Materializer, SourceShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.client.LedgerClient
import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.trigger.Runner.{TriggerContext, TriggerContextualFlow}
import com.daml.lf.engine.trigger.UnfoldState.{flatMapConcatNodeOps, toSourceOps}
import com.daml.lf.speedy.SValue.SList
import com.daml.lf.speedy.SValue
import com.daml.logging.LoggingContextOf
import com.daml.platform.services.time.TimeProviderType
import com.daml.util.Ctx

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object TriggerRuleMetrics {
  final case class EvaluationMetrics(
      steps: Long,
      submissions: Long,
      getTimes: Long,
      ruleEvaluation: FiniteDuration,
      stepIteratorMean: FiniteDuration,
      stepIteratorDelayMean: Option[FiniteDuration],
  )

  final case class ACSMetrics(
      activeContracts: Long,
      pendingContracts: Long,
  )

  final case class InFlightMetrics(
      commands: Long
  )

  final case class SubmissionMetrics(
      creates: Long,
      exercises: Long,
      createAndExercises: Long,
      exerciseByKeys: Long,
  )

  final case class InternalStateMetrics(
      acs: ACSMetrics,
      inFlight: InFlightMetrics,
  )

  final case class RuleMetrics(
      evaluation: EvaluationMetrics,
      submission: SubmissionMetrics,
      startState: InternalStateMetrics,
      endState: InternalStateMetrics,
  )
}

final class TriggerRuleSimulationLib private (
    compiledPackages: PureCompiledPackages,
    triggerId: Identifier,
    triggerConfig: TriggerRunnerConfig,
    client: LedgerClient,
    timeProviderType: TimeProviderType,
    applicationId: ApplicationId,
    triggerParties: TriggerParties,
) {

  private[this] val logObserver: (String, TriggerLogContext) => Unit = { (_, _) =>
    // TODO: a future PR will provide a more meaningful implementation
  }

  private[this] implicit val materializer: Materializer = Materializer(
    ActorSystem("TriggerRuleSimulator")
  )
  private[this] implicit val loggingContext: LoggingContextOf[Trigger] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label[Trigger])(identity)
  private[this] implicit val triggerContext: TriggerLogContext =
    TriggerLogContext.newRootSpanWithCallback("trigger", logObserver)(identity)

  private val trigger = Trigger.fromIdentifier(compiledPackages, triggerId).toOption.get

  // We only perform rule simulation for recent high level triggers
  require(trigger.defn.level == Trigger.Level.High)
  require(trigger.defn.version > Trigger.Version.`2.0.0`)
  // For rule simulations, trigger runners should be configured with all hard limits enabled
  require(triggerConfig.hardLimit.allowTriggerTimeouts)
  require(triggerConfig.hardLimit.allowInFlightCommandOverflows)

  private[this] val runner = Runner(
    compiledPackages,
    trigger,
    triggerConfig,
    client,
    timeProviderType,
    applicationId,
    triggerParties,
  )

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def initialStateLambda(
      acs: Seq[CreatedEvent]
  ): Future[(Seq[TriggerContext[SubmitRequest]], SValue)] = {
    triggerContext.childSpan("simulation") { implicit triggerContext: TriggerLogContext =>
      triggerContext.childSpan("initialStateLambda") { implicit triggerContext: TriggerLogContext =>
        def initStateGraph = GraphDSL.createGraph(Sink.last[SValue]) {
          implicit gb => saveLastState =>
            import GraphDSL.Implicits._

            val clientTime: Timestamp =
              Timestamp.assertFromInstant(
                Runner.getTimeProvider(RunnerConfig.DefaultTimeProviderType).getCurrentTime
              )
            val killSwitch = KillSwitches.shared("init-state-simulation")
            val initialState = gb add runner.runInitialState(clientTime, killSwitch)(acs)
            val submissions = gb add Flow[TriggerContext[SubmitRequest]]

            initialState.finalState ~> saveLastState
            initialState.elemsOut ~> submissions

            new SourceShape(submissions.out)
        }

        def initStateSimulation = Source.fromGraph(initStateGraph)

        val submissions = initStateSimulation.runWith(Sink.seq)
        val initState = initStateSimulation.toMat(Sink.ignore)(Keep.left).run()

        submissions.zip(initState)
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def updateStateLambda(
      state: SValue,
      message: SValue,
  ): Future[(Seq[TriggerContext[SubmitRequest]], SValue)] = {
    triggerContext.childSpan("simulation") { implicit triggerContext: TriggerLogContext =>
      triggerContext.childSpan("updateStateLambda") { implicit triggerContext: TriggerLogContext =>
        def updateStateGraph = GraphDSL.createGraph(Sink.last[SValue]) {
          implicit gb => saveLastState =>
            import GraphDSL.Implicits._

            val lambdaKillSwitch = KillSwitches.shared("update-state-simulation")
            val msgIn = gb add TriggerContextualFlow[SValue].map(ctx =>
              ctx.copy(value = SList(FrontStack(ctx.value)))
            )
            val encodeMsg =
              gb add runner.encodeMsgs.map(ctx => ctx.copy(value = SList(FrontStack(ctx.value))))
            val stateOut = gb add Source.single(state)
            val rule = gb add runner.runRuleOnMsgs(lambdaKillSwitch)
            val killSwitch = gb add lambdaKillSwitch.flow[TriggerContext[SValue]]
            val submissions = gb add Flow[TriggerContext[SubmitRequest]]

            // format: off
            stateOut ~> rule.initState
            msgIn ~> killSwitch ~> rule.elemsIn
            submissions <~ rule.elemsOut
            rule.finalStates ~> saveLastState
            // format: on

            new FlowShape(msgIn.in, submissions.out)
        }

        def updateStateSimulation = Source
          .single(Ctx(triggerContext, message))
          .viaMat(Flow.fromGraph(updateStateGraph))(Keep.right)

        val submissions = updateStateSimulation.runWith(Sink.seq)
        val nextState = updateStateSimulation.toMat(Sink.ignore)(Keep.left).run()

        submissions.zip(nextState)
      }
    }
  }
}

object TriggerRuleSimulationLib {
  def getSimulator(
      client: LedgerClient,
      name: QualifiedName,
      packageId: PackageId,
      applicationId: ApplicationId,
      compiledPackages: PureCompiledPackages,
      timeProviderType: TimeProviderType,
      triggerConfig: TriggerRunnerConfig,
      actAs: String,
      readAs: Set[String] = Set.empty,
  ): (TriggerDefinition, TriggerRuleSimulationLib) = {
    val triggerId = Identifier(packageId, name)
    val simulator = new TriggerRuleSimulationLib(
      compiledPackages,
      triggerId,
      triggerConfig,
      client,
      timeProviderType,
      applicationId,
      TriggerParties(
        actAs = Party(actAs),
        readAs = Party.subst(readAs),
      ),
    )

    (simulator.trigger.defn, simulator)
  }
}
