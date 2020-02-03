// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics
import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.kvutils.DamlStateMap
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.daml.lf.data.Time
import org.slf4j.{Logger, LoggerFactory}

/** A committer processes a submission, with its inputs into an ordered set of output state and a log entry.
  * It is parametrized by the submission type `Submission` (e.g. PackageUploadEntry) and a committer's partial result
  * `PartialResult`.
  *
  * A committer implementation defines an initial partial result with `init` and `steps` to process the submission
  * into a set of DAML state outputs and a log entry. The main rationale behind this abstraction is to provide uniform
  * approach to implementing a kvutils committer that shares the handling of input and output DAML state, rejecting
  * a submission, logging and metrics.
  *
  * Each step is invoked with [[CommitContext]], that allows it to [[CommitContext.get]] and [[CommitContext.set]] daml state, and the
  * partial result from previous step.
  *
  * The result from a step is either [[StepContinue]] to continue to next step with new partial result, or [[StepStop]]
  * to finish the commit. A committer must produce a [[StepStop]] from one of the steps.
  *
  * Each committer is assigned its own logger (according to class name) and a set of metrics under
  * e.g. `kvutils.PackageCommitter`. An overall run time is measured in `kvutils.PackageCommitter.run-timer`,
  * and each step is measured separately under `step-timers.<step>`, e.g. `kvutils.PackageCommitter.step-timers.validateEntry`.
  */
private[kvutils] trait Committer[Submission, PartialResult] {
  type StepInfo = String
  type Step = (CommitContext, PartialResult) => StepResult[PartialResult]
  def steps: Iterable[(StepInfo, Step)]
  lazy val committerName: String = this.getClass.getSimpleName.toLowerCase

  /** The initial internal state passed to first step. */
  def init(ctx: CommitContext, subm: Submission): PartialResult

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //TODO: Replace with metrics registry object passed in constructor
  val metricsRegistry: metrics.MetricRegistry =
    metrics.SharedMetricRegistries.getOrCreate("kvutils")
  def metricsName(metric: String): String =
    metrics.MetricRegistry.name("kvutils.committer", committerName, metric)
  private val runTimer: Timer = metricsRegistry.timer(metricsName("run_timer"))
  private lazy val stepTimers: Map[StepInfo, Timer] =
    steps.map {
      case (info, _) =>
        info -> metricsRegistry.timer(metricsName(s"step_timers.${info}"))
    }.toMap

  /** A committer can `run` a submission and produce a log entry and output states. */
  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  def run(
      entryId: DamlLogEntryId,
      maximumRecordTime: Time.Timestamp,
      recordTime: Time.Timestamp,
      submission: Submission,
      participantId: ParticipantId,
      inputState: DamlStateMap,
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    runTimer.time { () =>
      val ctx = new CommitContext {
        override def getEntryId: DamlLogEntryId = entryId
        override def getMaximumRecordTime: Time.Timestamp = maximumRecordTime
        override def getRecordTime: Time.Timestamp = recordTime
        override def getParticipantId: ParticipantId = participantId
        override def inputs: DamlStateMap = inputState
      }
      var cstate = init(ctx, submission)
      for ((info, step) <- steps) {
        val result: StepResult[PartialResult] =
          stepTimers(info).time(() => step(ctx, cstate))
        result match {
          case StepContinue(newCState) => cstate = newCState
          case StepStop(logEntry) =>
            return logEntry -> ctx.getOutputs.toMap
        }
      }
      sys.error(s"Internal error: Committer ${this.getClass} did not produce a result!")
    }
}
