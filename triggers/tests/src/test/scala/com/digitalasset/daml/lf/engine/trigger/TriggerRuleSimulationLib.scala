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
import com.daml.logging.entries.LoggingValue
import com.daml.logging.entries.LoggingValue.{Nested, OfInt, OfLong, OfString}
import com.daml.platform.services.time.TimeProviderType
import com.daml.scalautil.Statement.discard
import com.daml.script.converter.Converter.Implicits._
import com.daml.util.Ctx

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

@SuppressWarnings(
  Array(
    "org.wartremover.warts.JavaSerializable",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
  )
)
private class TriggerRuleMetrics {

  private[this] var metricCountData = mutable.Map.empty[Set[UUID], mutable.Map[String, Long]]
  private[this] var metricTimingData =
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

  def clearMetrics(): Unit = {
    metricCountData = mutable.Map.empty
    metricTimingData = mutable.Map.empty
  }

  def getMetrics: TriggerRuleMetrics.RuleMetrics = {
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
