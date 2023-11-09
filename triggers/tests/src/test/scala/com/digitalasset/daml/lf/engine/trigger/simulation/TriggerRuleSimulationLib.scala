// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package simulation

import org.apache.pekko.stream.{FlowShape, KillSwitches, Materializer, RestartSettings, SourceShape}
import org.apache.pekko.stream.scaladsl.{Flow, GraphDSL, Keep, RestartSource, Sink, Source}
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
import com.daml.lf.speedy.{Command, SValue}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.logging.entries.LoggingValue
import com.daml.logging.entries.LoggingValue.{Nested, OfInt, OfLong, OfString}
import com.daml.platform.services.time.TimeProviderType
import com.daml.scalautil.Statement.discard
import com.daml.script.converter.Converter.Implicits._
import com.daml.util.Ctx
import org.scalacheck.Gen
import org.scalatest.Assertion
import org.scalatest.Assertions.succeed
import scalaz.Tag
import scalaz.syntax.tag._

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

@SuppressWarnings(
  Array(
    "org.wartremover.warts.JavaSerializable",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
  )
)
private class TriggerRuleMetrics {

  private[this] val metricCountData = mutable.Map.empty[Set[UUID], mutable.Map[String, Long]]
  private[this] val metricTimingData =
    mutable.Map.empty[Set[UUID], mutable.Map[String, FiniteDuration]]

  def addLogEntry(msg: String, context: TriggerLogContext): Unit = {
    addSteps(context)
    addSubmissions(context)
    addGetTime(context)
    addACSActiveStart(msg, context)
    addACSActiveEnd(msg, context)
    addACSPendingStart(msg, context)
    addACSPendingEnd(msg, context)
    addInFlightStart(msg, context)
    addInFlightEnd(msg, context)
    addRuleEvaluation(context)
    addStepIteratorMean(context)
    addStepIteratorDelayMean(context)
  }

  def getMetrics(
      retries: Int = 5
  )(implicit materializer: Materializer): Future[TriggerRuleMetrics.RuleMetrics] = {
    val backoff = 50.milliseconds
    val timeLimit = backoff * (1L to retries.toLong).sum
    val restartSettings =
      RestartSettings(minBackoff = backoff, maxBackoff = 1.second, randomFactor = 0.1)
        .withMaxRestarts(retries, timeLimit)

    RestartSource
      .withBackoff(restartSettings) { () =>
        Source.single(getMetrics)
      }
      .initialTimeout(timeLimit)
      .runWith(Sink.head)
  }

  private[this] def getMetrics: TriggerRuleMetrics.RuleMetrics = {
    import TriggerRuleMetrics._

    require(
      metricCountData.keys.toSet.size == 1,
      s"Metric count data was associated with multiple spans: ${metricCountData.keys.toSet}",
    )

    val (spanId, countMetrics) = metricCountData.head

    require(
      metricTimingData.keys.toSet == metricCountData.keys.toSet,
      s"Timing metric data was associated with different spans to count metric data: for span ID $spanId found the keys ${metricTimingData.keys.toSet}",
    )

    val timingMetrics = metricTimingData(spanId)

    require(
      countMetrics.keys.toSet == Set(
        "acs-active-start",
        "acs-active-end",
        "acs-pending-start",
        "acs-pending-end",
        "in-flight-start",
        "in-flight-end",
        "steps",
        "get-time",
        "submission-total",
        "submission-create",
        "submission-exercise",
        "submission-create-and-exercise",
        "submission-exercise-by-key",
      ),
      s"Count metric data did not contain all the expected values: for span ID $spanId found the keys ${countMetrics.keys.toSet}",
    )
    require(
      timingMetrics.keys.toSet
        .subsetOf(
          Set(
            "rule-evaluation",
            "step-iterator-mean",
            "step-iterator-delay-mean",
          )
        ),
      s"Timing metric data contained unexpected values: for span ID $spanId found the keys ${timingMetrics.keys.toSet}",
    )
    require(
      Set(
        "rule-evaluation",
        "step-iterator-mean",
      ).subsetOf(timingMetrics.keys.toSet),
      s"Timing metric data was missing expected values: for span ID $spanId found the keys ${timingMetrics.keys.toSet}",
    )

    RuleMetrics(
      evaluation = EvaluationMetrics(
        steps = countMetrics("steps"),
        submissions = countMetrics("submission-total"),
        getTimes = countMetrics("get-time"),
        ruleEvaluation = timingMetrics("rule-evaluation"),
        stepIteratorMean = timingMetrics("step-iterator-mean"),
        stepIteratorDelayMean = timingMetrics.get("step-iterator-delay-mean"),
      ),
      submission = SubmissionMetrics(
        creates = countMetrics("submission-create"),
        exercises = countMetrics("submission-exercise"),
        createAndExercises = countMetrics("submission-create-and-exercise"),
        exerciseByKeys = countMetrics("submission-exercise-by-key"),
      ),
      startState = InternalStateMetrics(
        acs = ACSMetrics(
          activeContracts = countMetrics("acs-active-start"),
          pendingContracts = countMetrics("acs-pending-start"),
        ),
        inFlight = InFlightMetrics(
          commands = countMetrics("in-flight-start")
        ),
      ),
      endState = InternalStateMetrics(
        acs = ACSMetrics(
          activeContracts = countMetrics("acs-active-end"),
          pendingContracts = countMetrics("acs-pending-end"),
        ),
        inFlight = InFlightMetrics(
          commands = countMetrics("in-flight-end")
        ),
      ),
    )
  }

  private def addACSActiveStart(msg: String, context: TriggerLogContext): Unit = discard {
    if ("Trigger rule .+ start".r.matches(msg)) {
      for {
        count <- getACSActive(context)
      } yield {
        metricCountData.getOrElseUpdate(
          Set(context.span.id),
          mutable.Map.empty,
        ) += ("acs-active-start" -> count)
      }
    }
  }

  private def addACSActiveEnd(msg: String, context: TriggerLogContext): Unit = discard {
    if ("Trigger rule .+ end".r.matches(msg)) {
      for {
        count <- getACSActive(context)
      } yield {
        metricCountData.getOrElseUpdate(
          Set(context.span.id),
          mutable.Map.empty,
        ) += ("acs-active-end" -> count)
      }
    }
  }

  private def addACSPendingStart(msg: String, context: TriggerLogContext): Unit = discard {
    if ("Trigger rule .+ start".r.matches(msg)) {
      for {
        count <- getACSPending(context)
      } yield {
        metricCountData.getOrElseUpdate(
          Set(context.span.id),
          mutable.Map.empty,
        ) += ("acs-pending-start" -> count)
      }
    }
  }

  private def addACSPendingEnd(msg: String, context: TriggerLogContext): Unit = discard {
    if ("Trigger rule .+ end".r.matches(msg)) {
      for {
        count <- getACSPending(context)
      } yield {
        metricCountData.getOrElseUpdate(
          Set(context.span.id),
          mutable.Map.empty,
        ) += ("acs-pending-end" -> count)
      }
    }
  }

  private def addInFlightStart(msg: String, context: TriggerLogContext): Unit = discard {
    if ("Trigger rule .+ start".r.matches(msg)) {
      for {
        count <- getInFlight(context)
      } yield {
        metricCountData.getOrElseUpdate(
          Set(context.span.id),
          mutable.Map.empty,
        ) += ("in-flight-start" -> count)
      }
    }
  }

  private def addInFlightEnd(msg: String, context: TriggerLogContext): Unit = discard {
    if ("Trigger rule .+ end".r.matches(msg)) {
      for {
        count <- getInFlight(context)
      } yield {
        metricCountData.getOrElseUpdate(
          Set(context.span.id),
          mutable.Map.empty,
        ) += ("in-flight-end" -> count)
      }
    }
  }

  private def addSteps(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      steps <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("steps") => entries.contents("steps")
          },
        )
      count <- steps
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("steps" -> count)
    }
  }

  private def addRuleEvaluation(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      duration <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("duration") =>
              entries.contents("duration")
          },
        )
      ruleEval <- duration
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("rule-evaluation") =>
              entries.contents("rule-evaluation")
          },
        )
      value <- ruleEval
        .expect("LoggingValue.OfString", { case OfString(value) => value })
      timing <- Try(
        Duration(value.replace('u', 'µ')).asInstanceOf[FiniteDuration]
      ).toEither
    } yield {
      metricTimingData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("rule-evaluation" -> timing)
    }
  }

  private def addStepIteratorMean(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      duration <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("duration") =>
              entries.contents("duration")
          },
        )
      ruleEval <- duration
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("step-iterator-mean") =>
              entries.contents("step-iterator-mean")
          },
        )
      value <- ruleEval
        .expect("LoggingValue.OfString", { case OfString(value) => value })
      timing <- Try(
        Duration(value.replace('u', 'µ')).asInstanceOf[FiniteDuration]
      ).toEither
    } yield {
      metricTimingData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("step-iterator-mean" -> timing)
    }
  }

  private def addStepIteratorDelayMean(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      duration <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("duration") =>
              entries.contents("duration")
          },
        )
      ruleEval <- duration
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("step-iterator-delay-mean") =>
              entries.contents("step-iterator-delay-mean")
          },
        )
      value <- ruleEval
        .expect("LoggingValue.OfString", { case OfString(value) => value })
      timing <- Try(
        Duration(value.replace('u', 'µ')).asInstanceOf[FiniteDuration]
      ).toEither
    } yield {
      metricTimingData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("step-iterator-delay-mean" -> timing)
    }
  }

  private def addSubmissions(context: TriggerLogContext): Unit = {
    addSubmissionTotal(context)
    addSubmissionCreate(context)
    addSubmissionExercise(context)
    addSubmissionCreateAndExercise(context)
    addSubmissionExerciseByKey(context)
  }

  private def addGetTime(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      getTimes <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("get-time") =>
              entries.contents("get-time")
          },
        )
      count <- getTimes
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("get-time" -> count)
    }
  }

  private def addSubmissionTotal(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      submissions <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("submissions") =>
              entries.contents("submissions")
          },
        )
      total <- submissions
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("total") => entries.contents("total")
          },
        )
      count <- total
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("submission-total" -> count)
    }
  }

  private def addSubmissionCreate(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      submissions <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("submissions") =>
              entries.contents("submissions")
          },
        )
      create <- submissions
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("create") =>
              entries.contents("create")
          },
        )
      count <- create
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("submission-create" -> count)
    }
  }

  private def addSubmissionExercise(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      submissions <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("submissions") =>
              entries.contents("submissions")
          },
        )
      exercise <- submissions
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("exercise") =>
              entries.contents("exercise")
          },
        )
      count <- exercise
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("submission-exercise" -> count)
    }
  }

  private def addSubmissionCreateAndExercise(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      submissions <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("submissions") =>
              entries.contents("submissions")
          },
        )
      createAndExercise <- submissions
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("createAndExercise") =>
              entries.contents("createAndExercise")
          },
        )
      count <- createAndExercise
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("submission-create-and-exercise" -> count)
    }
  }

  private def addSubmissionExerciseByKey(context: TriggerLogContext): Unit = discard {
    for {
      metrics <- getMetrics(context)
      submissions <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("submissions") =>
              entries.contents("submissions")
          },
        )
      exerciseByKey <- submissions
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("exerciseByKey") =>
              entries.contents("exerciseByKey")
          },
        )
      count <- exerciseByKey
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield {
      metricCountData.getOrElseUpdate(
        context.span.parent,
        mutable.Map.empty,
      ) += ("submission-exercise-by-key" -> count)
    }
  }

  private def getMetrics(context: TriggerLogContext): Either[String, LoggingValue] = {
    context.entries
      .find(_._1 == "metrics")
      .map(_._2)
      .toRight("Trigger logging context did not have a metrics block")
  }

  private def getACSActive(context: TriggerLogContext): Either[String, Long] = {
    for {
      metrics <- getMetrics(context)
      acs <- metrics
        .expect(
          "LoggingValue.Nested",
          { case Nested(entries) if entries.contents.contains("acs") => entries.contents("acs") },
        )
      active <- acs
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("active") =>
              entries.contents("active")
          },
        )
      result <- active
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield result
  }

  private def getACSPending(context: TriggerLogContext): Either[String, Long] = {
    for {
      metrics <- getMetrics(context)
      acs <- metrics
        .expect(
          "LoggingValue.Nested",
          { case Nested(entries) if entries.contents.contains("acs") => entries.contents("acs") },
        )
      pending <- acs
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("pending") =>
              entries.contents("pending")
          },
        )
      result <- pending
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield result
  }

  private def getInFlight(context: TriggerLogContext): Either[String, Long] = {
    for {
      metrics <- getMetrics(context)
      inFlight <- metrics
        .expect(
          "LoggingValue.Nested",
          {
            case Nested(entries) if entries.contents.contains("in-flight") =>
              entries.contents("in-flight")
          },
        )
      result <- inFlight
        .expect(
          "LoggingValue.OfNumeric",
          {
            case OfInt(value) => value.toLong
            case OfLong(value) => value
          },
        )
    } yield result
  }
}

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

final class TriggerRuleSimulationLib private[simulation] (
    compiledPackages: PureCompiledPackages,
    triggerId: Identifier,
    triggerConfig: TriggerRunnerConfig,
    client: LedgerClient,
    timeProviderType: TimeProviderType,
    applicationId: ApplicationId,
    triggerParties: TriggerParties,
)(implicit materializer: Materializer) {

  private[this] implicit val logger: ContextualizedLogger =
    ContextualizedLogger.get(classOf[Runner])
  private[this] implicit val executionContext: ExecutionContext = materializer.executionContext
  private[this] implicit val loggingContext: LoggingContextOf[Trigger] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label[Trigger])(identity)

  private[simulation] val trigger = Trigger
    .fromIdentifier(compiledPackages, triggerId)(
      TriggerLogContext.newRootSpan(
        "simulation",
        "id" -> triggerId.toString,
        "applicationId" -> applicationId.unwrap,
        "definition" -> triggerId.toString,
        "readAs" -> Tag.unsubst(triggerParties.readAs),
        "actAs" -> Tag.unwrap(triggerParties.actAs),
      )(identity)
    )
    .toOption
    .get

  // We only perform rule simulation for recent high level triggers
  require(trigger.defn.level == Trigger.Level.High)
  require(trigger.defn.version > Trigger.Version.`2.0.0`)
  // For rule simulations, trigger runners should be configured with all hard limits enabled
  require(triggerConfig.hardLimit.allowTriggerTimeouts)
  require(triggerConfig.hardLimit.allowInFlightCommandOverflows)

  class SimulationContext {
    private[simulation] val ruleMetrics = new TriggerRuleMetrics
    private[this] val logObserver: (String, TriggerLogContext) => Unit = { (msg, context) =>
      ruleMetrics.addLogEntry(msg, context)
    }

    private[simulation] implicit val triggerContext: TriggerLogContext =
      TriggerLogContext.newRootSpanWithCallback(
        "simulation",
        logObserver,
        "id" -> triggerId.toString,
        "applicationId" -> applicationId.unwrap,
        "definition" -> triggerId.toString,
        "readAs" -> Tag.unsubst(triggerParties.readAs),
        "actAs" -> Tag.unwrap(triggerParties.actAs),
      )(identity)

    private[simulation] val runner = Runner(
      compiledPackages,
      trigger,
      triggerConfig,
      client,
      timeProviderType,
      applicationId,
      triggerParties,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def initialStateLambda(
      acs: Seq[CreatedEvent]
  ): Future[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue)] = {
    val context = new SimulationContext
    val initStateGraph = context.triggerContext.childSpan("initialStateLambda") {
      implicit triggerContext: TriggerLogContext =>
        GraphDSL.createGraph(Sink.last[SValue]) { implicit gb => saveLastState =>
          import GraphDSL.Implicits._

          val clientTime: Timestamp =
            Timestamp.assertFromInstant(
              Runner.getTimeProvider(RunnerConfig.DefaultTimeProviderType).getCurrentTime
            )
          val killSwitch = KillSwitches.shared("init-state-simulation")
          val initialState = gb add context.runner.runInitialState(clientTime, killSwitch)(acs)
          val submissions = gb add Flow[TriggerContext[SubmitRequest]]

          initialState.finalState ~> saveLastState
          initialState.elemsOut ~> submissions

          new SourceShape(submissions.out)
        }
    }
    val initStateSimulation = Source.fromGraph(initStateGraph)
    val (initState, submissions) = initStateSimulation.toMat(Sink.seq)(Keep.both).run()

    submissions.map(_.map(_.value)).zip(initState).flatMap { case (requests, state) =>
      context.ruleMetrics.getMetrics().map((requests, _, state))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def updateStateLambda(
      state: SValue,
      message: TriggerMsg,
  ): Future[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue)] = {
    val context = new SimulationContext
    val updateStateGraph = context.triggerContext.childSpan("updateStateLambda") {
      implicit triggerContext: TriggerLogContext =>
        GraphDSL.createGraph(Sink.last[SValue]) { implicit gb => saveLastState =>
          import GraphDSL.Implicits._

          val lambdaKillSwitch = KillSwitches.shared("update-state-simulation")
          val msgIn = gb add TriggerContextualFlow[TriggerMsg]
          val encodeMsg =
            gb add context.runner.encodeMsgs.map(ctx =>
              ctx.copy(value = SList(FrontStack(ctx.value)))
            )
          val stateOut = gb add Source.single(state)
          val rule = gb add context.runner.runRuleOnMsgs(lambdaKillSwitch)
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
    }
    val updateStateSimulation = Source
      .single(Ctx(context.triggerContext, message))
      .viaMat(Flow.fromGraph(updateStateGraph))(Keep.right)
    val (nextState, submissions) = updateStateSimulation.toMat(Sink.seq)(Keep.both).run()

    submissions.map(_.map(_.value)).zip(nextState).flatMap { case (requests, state) =>
      context.ruleMetrics.getMetrics().map((requests, _, state))
    }
  }
}

object TriggerRuleSimulationLib {

  type CommandsInFlight = Map[String, Seq[Command]]

  final case class TriggerExperiment(
      acsGen: Gen[Seq[CreatedEvent]],
      userStateGen: Gen[SValue],
      inFlightCmdGen: Gen[CommandsInFlight],
      msgGen: Gen[TriggerMsg],
  ) {
    val initState: Gen[Seq[CreatedEvent]] = acsGen

    val updateState: Gen[(Seq[CreatedEvent], SValue, CommandsInFlight, TriggerMsg)] =
      for {
        acs <- acsGen
        userState <- userStateGen
        inFlightCmds <- inFlightCmdGen
        msg <- msgGen
      } yield (acs, userState, inFlightCmds, msg)
  }

  def forAll[T](gen: Gen[T], sampleSize: Int = 100, parallelism: Int = 1)(
      test: T => Future[Assertion]
  )(implicit materializer: Materializer): Future[Assertion] = {
    Source(0 to sampleSize)
      .map(_ => gen.sample)
      .collect { case Some(data) => data }
      .mapAsync(parallelism) { data =>
        test(data)
      }
      .takeWhile(_ == succeed, inclusive = true)
      .runWith(Sink.last)
  }

  def forAll[T](gen: Iterator[T], parallelism: Int)(
      test: T => Future[Assertion]
  )(implicit materializer: Materializer): Future[Assertion] = {
    Source
      .fromIterator(() => gen)
      .mapAsync(parallelism = parallelism) { data =>
        test(data)
      }
      .takeWhile(_ == succeed, inclusive = true)
      .runWith(Sink.last)
  }

  def forAll[T](gen: Iterator[T])(
      test: T => Future[Assertion]
  )(implicit materializer: Materializer): Future[Assertion] = {
    forAll[T](gen, 1)(test)
  }

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
  )(implicit materializer: Materializer): (TriggerDefinition, TriggerRuleSimulationLib) = {
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

  def numberOfCreateCommands(request: SubmitRequest): Int = {
    request.getCommands.commands.count(_.command.isCreate)
  }

  def numberOfCreateAndExerciseCommands(request: SubmitRequest): Int = {
    request.getCommands.commands.count(_.command.isCreateAndExercise)
  }

  def numberOfExerciseCommands(request: SubmitRequest): Int = {
    request.getCommands.commands.count(_.command.isExercise)
  }

  def numberOfExerciseByKeyCommands(request: SubmitRequest): Int = {
    request.getCommands.commands.count(_.command.isExerciseByKey)
  }
}
