// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.stream.{FlowShape, KillSwitches, Materializer, SourceShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.lf.CompiledPackages
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.trigger.Runner.{TriggerContext, TriggerContextualFlow}
import com.daml.lf.engine.trigger.UnfoldState.{flatMapConcatNodeOps, toSourceOps}
import com.daml.lf.speedy.SExpr.SEValue
import com.daml.lf.speedy.SValue.{SList, SUnit}
import com.daml.lf.speedy.{SValue, Speedy}
import com.daml.logging.LoggingContextOf
import com.daml.scalautil.Statement.discard
import com.daml.script.converter.Converter.Implicits._
import com.daml.util.Ctx
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import spray.json._

import scala.util.control.NonFatal

private class TriggerRuleMetrics {

  private[this] var metricCountData = Map.empty[String, Long]
  private[this] var metricTimingData = Map.empty[String, FiniteDuration]

  def addLogEntry(logEntry: JsObject): Unit = {
    addSteps(logEntry)
    addRuleEvaluation(logEntry)
    addStepIteratorMean(logEntry)
    addStepIteratorDelayMean(logEntry)
  }

  def clearMetrics(): Unit = {
    metricCountData = Map.empty
    metricTimingData = Map.empty
  }

  def getMetrics: TriggerRuleMetrics.RuleMetrics = {
    ???
  }

  private def addSteps(logEntry: JsObject): Unit = {
    try {
      val count = logEntry
        .fields("contents")
        .asJsObject
        .fields("trigger")
        .asJsObject
        .fields("metrics")
        .asJsObject
        .fields("steps")
        .expect(
          "JsNumber",
          { case JsNumber(value) =>
            value.longValue
          },
        )
        .orConverterException

      metricCountData += ("steps" -> count)
    } catch {
      case NonFatal(_) =>
    }
  }

  private def addRuleEvaluation(logEntry: JsObject): Unit = {
    try {
      val timing = logEntry
        .fields("contents")
        .asJsObject
        .fields("trigger")
        .asJsObject
        .fields("metrics")
        .asJsObject
        .fields("duration")
        .asJsObject
        .fields("rule-evaluation")
        .expect(
          "JsString",
          { case JsString(value) =>
            Duration(value).asInstanceOf[FiniteDuration]
          },
        )
        .orConverterException

      metricTimingData += ("rule-evaluation" -> timing)
    } catch {
      case NonFatal(_) =>
    }
  }

  private def addStepIteratorMean(logEntry: JsObject): Unit = {
    try {
      val timing = logEntry
        .fields("contents")
        .asJsObject
        .fields("trigger")
        .asJsObject
        .fields("metrics")
        .asJsObject
        .fields("duration")
        .asJsObject
        .fields("step-iterator-mean")
        .expect(
          "JsString",
          { case JsString(value) =>
            Duration(value).asInstanceOf[FiniteDuration]
          },
        )
        .orConverterException

      metricTimingData += ("step-iterator-mean" -> timing)
    } catch {
      case NonFatal(_) =>
    }
  }

  private def addStepIteratorDelayMean(logEntry: JsObject): Unit = {
    try {
      val timing = logEntry
        .fields("contents")
        .asJsObject
        .fields("trigger")
        .asJsObject
        .fields("metrics")
        .asJsObject
        .fields("duration")
        .asJsObject
        .fields("step-iterator-delay-mean")
        .expect(
          "JsString",
          { case JsString(value) =>
            Duration(value).asInstanceOf[FiniteDuration]
          },
        )
        .orConverterException

      metricTimingData += ("step-iterator-delay-mean" -> timing)
    } catch {
      case NonFatal(_) =>
    }
  }
}

object TriggerRuleMetrics {
  final case class EvaluationMetrics(
      steps: Long,
      ruleEvaluation: FiniteDuration,
      stepIteratorMean: FiniteDuration,
      stepIteratorDelayMean: Option[FiniteDuration],
  )

  final case class ACSMetrics(
      activeContracts: Long,
      activeContractAdditions: Long,
      activeContractDeletions: Long,
      pendingContracts: Long,
      pendingContractAdditions: Long,
      pendingContractDeletions: Long,
  )

  final case class SubmissionMetrics(
      submissions: Long,
      inFlightCommands: Long,
      inFlightCommandAdditions: Long,
      inFlightCommandRemovals: Long,
  )

  final case class RuleMetrics(
      evaluation: EvaluationMetrics,
      acs: ACSMetrics,
      submission: SubmissionMetrics,
  )
}

final class TriggerRuleSimulationLib[UserState](
    compiledPackages: CompiledPackages,
    triggerConfig: TriggerRunnerConfig,
    level: Trigger.Level,
    version: Trigger.Version,
    runner: Runner,
) {

  // We only perform rule simulation for recent high level triggers
  require(level == Trigger.Level.High)
  require(version > Trigger.Version.`2.0.0`)
  // For rule simulations, trigger runners should be configured with all hard limits enabled
  require(triggerConfig.hardLimit.allowTriggerTimeouts)
  require(triggerConfig.hardLimit.allowInFlightCommandOverflows)

  private[this] val loggingContext: LoggingContextOf[Trigger] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label[Trigger])(identity)
  private[this] val triggerRunnerLogger: Logger =
    LoggerFactory.getLogger(classOf[Runner]).asInstanceOf[Logger]
  private val ruleMetrics = new TriggerRuleMetrics

  private[this] implicit val materializer: Materializer = Materializer(
    ActorSystem("TriggerRuleSimulator")
  )
  private[this] implicit val executionContext: ExecutionContext = materializer.executionContext
  private[this] implicit val triggerContext: TriggerLogContext =
    TriggerLogContext.newRootSpan("simulation")(identity)(loggingContext)
  private[this] implicit val machine: Speedy.PureMachine =
    Speedy.Machine.fromPureSExpr(compiledPackages, SEValue(SUnit))(loggingContext)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def initialStateLambda(
      acs: Seq[CreatedEvent]
  ): Future[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue)] = {
    val logAppender = setupTriggerMetricsLogging()
    try {
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

      submissions.map(_.map(_.value)).zip(initState).map { case (submissions, state) =>
        parseLoggedMetrics(logAppender)
        (submissions, ruleMetrics.getMetrics, state)
      }
    } finally {
      removeTriggerMetricsLogging(logAppender)
      ruleMetrics.clearMetrics()
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def updateStateLambda(
      state: SValue,
      message: TriggerMsg,
  ): Future[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue)] = {
    val logAppender = setupTriggerMetricsLogging()
    try {
      val lambdaKillSwitch = KillSwitches.shared("update-state-simulation")
      val updateStateGraph = GraphDSL.createGraph(Sink.last[SValue]) {
        implicit gb => saveLastState =>
          import GraphDSL.Implicits._

          val msgIn = gb add TriggerContextualFlow[TriggerMsg]
          val encodeMsg =
            gb add runner.encodeMsgs.map(ctx => ctx.copy(value = SList(FrontStack(ctx.value))))
          val stateOut = gb add Source.single(state)
          val rule = gb add runner.runRuleOnMsgs(lambdaKillSwitch)
          val killSwitch = gb add lambdaKillSwitch.flow[TriggerContext[SValue]]
          val submissions = gb add Flow[TriggerContext[SubmitRequest]]

          // format: off
          stateOut                         ~> rule.initState
          msgIn ~> encodeMsg ~> killSwitch ~> rule.elemsIn
          submissions                      <~ rule.elemsOut
                                              rule.finalStates ~> saveLastState
          // format: on

          new FlowShape(msgIn.in, submissions.out)
      }
      val updateStateSimulation = Source
        .single(Ctx(triggerContext, message))
        .viaMat(Flow.fromGraph(updateStateGraph))(Keep.right)
      val submissions = updateStateSimulation.runWith(Sink.seq)
      val nextState = updateStateSimulation.toMat(Sink.ignore)(Keep.left).run()

      submissions.map(_.map(_.value)).zip(nextState).map { case (submissions, state) =>
        parseLoggedMetrics(logAppender)
        (submissions, ruleMetrics.getMetrics, state)
      }
    } finally {
      removeTriggerMetricsLogging(logAppender)
      ruleMetrics.clearMetrics()
    }
  }

  private def setupTriggerMetricsLogging(): ListAppender[ILoggingEvent] = {
    val triggerRunnerLogAppender = new ListAppender[ILoggingEvent]()

    triggerRunnerLogAppender.start()
    triggerRunnerLogger.addAppender(triggerRunnerLogAppender)

    triggerRunnerLogAppender
  }

  private def removeTriggerMetricsLogging(logAppender: ListAppender[ILoggingEvent]): Unit = {
    discard(triggerRunnerLogger.detachAppender(logAppender))
  }

  private def parseLoggedMetrics(triggerRunnerLogAppender: ListAppender[ILoggingEvent]): Unit = {
    triggerRunnerLogAppender.list.toArray.toVector.foreach {
      case event: ILoggingEvent =>
        event.getMarkerList.asScala.foreach { marker =>
          // TODO: validate logging context against a JSON schema?
          ruleMetrics.addLogEntry(marker.toString.parseJson.asJsObject)
        }

      case _ =>
    }
  }
}
