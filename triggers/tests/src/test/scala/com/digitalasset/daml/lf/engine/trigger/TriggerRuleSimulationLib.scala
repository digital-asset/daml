// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.stream.{FlowShape, KillSwitches, Materializer, SourceShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.trigger.Runner.{TriggerContext, TriggerContextualFlow}
import com.daml.lf.engine.trigger.ToLoggingContext._
import com.daml.lf.engine.trigger.UnfoldState.{flatMapConcatNodeOps, toSourceOps}
import com.daml.lf.speedy.SExpr.SEValue
import com.daml.lf.speedy.SValue.{SList, SUnit}
import com.daml.lf.speedy.{SValue, Speedy}
import com.daml.logging.LoggingContextOf
import com.daml.util.Ctx

import scala.concurrent.Future

class TriggerRuleSimulationLib[UserState](runner: Runner) {

  private[this] val loggingContext: LoggingContextOf[Trigger] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label[Trigger])(identity)

  private[this] implicit val materializer: Materializer = Materializer(
    ActorSystem("TriggerRuleSimulator")
  )
  private[this] implicit val triggerContext: TriggerLogContext =
    TriggerLogContext.newRootSpan("simulation")(identity)(loggingContext)
  private[this] implicit val machine: Speedy.PureMachine =
    Speedy.Machine.fromPureSExpr(runner.compiledPackages, SEValue(SUnit))

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def initialStateLambda(
      acs: Seq[CreatedEvent]
  ): Future[(Seq[TriggerContext[SubmitRequest]], SValue)] = {
    val clientTime: Timestamp =
      Timestamp.assertFromInstant(
        Runner.getTimeProvider(RunnerConfig.DefaultTimeProviderType).getCurrentTime
      )
    val killSwitch = KillSwitches.shared("init-state-simulation")
    val initStateGraph = GraphDSL.createGraph(Sink.last[SValue]) { implicit gb => saveLastState =>
      import GraphDSL.Implicits._

      val initialState = gb add runner.runInitialState(clientTime, killSwitch)(acs)
      val submissions = gb add Flow[TriggerContext[SubmitRequest]]

      initialState.finalState ~> saveLastState
      initialState.elemsOut ~> submissions

      new SourceShape(submissions.out)
    }
    val initStateSimulation = Source.fromGraph(initStateGraph)
    val submissions = initStateSimulation.runWith(Sink.seq)
    val initState = initStateSimulation.toMat(Sink.ignore)(Keep.left).run()

    submissions.zip(initState)
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def updateStateLambda(
      state: SValue,
      message: SValue,
  ): Future[(Seq[TriggerContext[SubmitRequest]], SValue)] = {
    val lambdaKillSwitch = KillSwitches.shared("update-state-simulation")
    val updateStateGraph = GraphDSL.createGraph(Sink.last[SValue]) { implicit gb => saveLastState =>
      import GraphDSL.Implicits._

      val msgIn = gb add TriggerContextualFlow[SValue].map(ctx =>
        ctx.copy(value = SList(FrontStack(ctx.value)))
      )
      val stateOut = gb add Source.single(state)
      val rule = gb add runner.runRuleOnMsgs(lambdaKillSwitch)
      val killSwitch = gb add lambdaKillSwitch.flow[TriggerContext[SValue]]
      val submissions = gb add Flow[TriggerContext[SubmitRequest]]

      // format: off
      stateOut            ~> rule.initState
      msgIn ~> killSwitch ~> rule.elemsIn
      submissions         <~ rule.elemsOut
                             rule.finalStates ~> saveLastState
      // format: on

      new FlowShape(msgIn.in, submissions.out)
    }
    val updateStateSimulation = Source
      .single(Ctx(triggerContext, message))
      .viaMat(Flow.fromGraph(updateStateGraph))(Keep.right)
    val submissions = updateStateSimulation.runWith(Sink.seq)
    val nextState = updateStateSimulation.toMat(Sink.ignore)(Keep.left).run()

    submissions.zip(nextState)
  }
}
