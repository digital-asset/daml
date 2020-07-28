// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationEntry,
  DamlLogEntry,
  DamlOutOfTimeBoundsEntry,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission
}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils._
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.daml.lf.data.Time
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import org.slf4j.{Logger, LoggerFactory}

/** A committer either processes or pre-executes a submission, with its inputs into an ordered set of output state and
  * a log entry or multiple log entries in case of pre-execution.
  * It is parametrized by the committer's partial result `PartialResult`.
  *
  * A committer implementation defines an initial partial result with init and `steps` to process the submission
  * into a set of DAML state outputs and a log entry. The main rationale behind this abstraction is to provide uniform
  * approach to implementing a kvutils committer that shares the handling of input and output DAML state, rejecting
  * a submission, logging and metrics.
  *
  * Each step is invoked with [[CommitContext]], that allows it to [[CommitContext.get]] and [[CommitContext.set]]
  * DAML state, and the partial result from previous step.
  * When processing a submission for pre-execution, a committer produces the following set of results:
  *   - a set of output states and a log entry to be applied in case of no conflicts and/or time-out,
  *   - a record time window within which the submission is considered valid and a deduplication
  * deadline (optional),
  *   - a rejection log entry to be sent back to the participant in case the record time at
  * sequencing the commit is out of the previous bounds.
  * The above pieces of information are set via the [[CommitContext]] as well.
  *
  * The result from a step is either [[StepContinue]] to continue to next step with new partial result, or [[StepStop]]
  * to finish the commit. A committer must produce a [[StepStop]] from one of the steps.
  *
  * Each committer is assigned its own logger (according to class name) and a set of metrics under
  * e.g. `kvutils.PackageCommitter`. An overall run time is measured in `kvutils.PackageCommitter.run-timer`,
  * and each step is measured separately under `step-timers.<step>`, e.g. `kvutils.PackageCommitter.step-timers.validateEntry`.
  *
  * @see [[com.daml.ledger.participant.state.kvutils.committer.CommitContext]]
  * @see [[com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult]]
  */
private[committer] trait Committer[PartialResult] extends SubmissionExecutor {
  protected final type Step = (CommitContext, PartialResult) => StepResult[PartialResult]

  protected final val logger: Logger = LoggerFactory.getLogger(getClass)

  protected val committerName: String

  protected def steps: Iterable[(StepInfo, Step)]

  /** The initial internal state passed to first step. */
  protected def init(ctx: CommitContext, submission: DamlSubmission): PartialResult

  protected val metrics: Metrics

  // These are lazy because they rely on `committerName`, which is defined in the subclass and
  // therefore not set at object initialization.
  private lazy val runTimer: Timer = metrics.daml.kvutils.committer.runTimer(committerName)
  private lazy val preExecutionRunTimer: Timer =
    metrics.daml.kvutils.committer.preExecutionRunTimer(committerName)
  private lazy val stepTimers: Map[StepInfo, Timer] =
    steps.map {
      case (info, _) =>
        info -> metrics.daml.kvutils.committer.stepTimer(committerName, info)
    }.toMap

  /** A committer can `run` a submission and produce a log entry and output states. */
  def run(
      recordTime: Option[Time.Timestamp],
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: DamlStateMap,
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    runTimer.time { () =>
      val commitContext: CommitContext = new CommitContext {
        override def getRecordTime: Option[Time.Timestamp] = recordTime

        override def getParticipantId: ParticipantId = participantId

        override val inputs: DamlStateMap = inputState
      }
      val logEntry = runSteps(commitContext, submission)
      logEntry -> commitContext.getOutputs.toMap
    }

  def runWithPreExecution(
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: DamlStateMap,
  ): PreExecutionResult =
    preExecutionRunTimer.time { () =>
      val commitContext: CommitContext = new CommitContext {
        override def getRecordTime: Option[Time.Timestamp] = None

        override def getParticipantId: ParticipantId = participantId

        override val inputs: DamlStateMap = inputState
      }
      preExecute(submission, participantId, inputState, commitContext)
    }

  private[committer] def preExecute(
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: DamlStateMap,
      commitContext: CommitContext,
  ): PreExecutionResult = {
    val successfulLogEntry = runSteps(commitContext, submission)
    PreExecutionResult(
      readSet = commitContext.getAccessedInputKeys.toSet,
      successfulLogEntry = successfulLogEntry,
      stateUpdates = commitContext.getOutputs.toMap,
      outOfTimeBoundsLogEntry = constructOutOfTimeBoundsLogEntry(commitContext),
      minimumRecordTime = commitContext.minimumRecordTime
        .map(Timestamp.assertFromInstant),
      maximumRecordTime = commitContext.maximumRecordTime
        .map(Timestamp.assertFromInstant)
    )
  }

  private def constructOutOfTimeBoundsLogEntry(commitContext: CommitContext): DamlLogEntry =
    commitContext.outOfTimeBoundsLogEntry
      .map { rejectionLogEntry =>
        val builder = DamlOutOfTimeBoundsEntry.newBuilder
          .setEntry(rejectionLogEntry)
        commitContext.minimumRecordTime.foreach { instant =>
          builder.setTooEarlyUntil(buildTimestamp(instant))
        }
        commitContext.maximumRecordTime.foreach { instant =>
          builder.setTooLateFrom(buildTimestamp(instant))
        }
        commitContext.deduplicateUntil.foreach { instant =>
          builder.setDuplicateUntil(buildTimestamp(instant))
        }
        DamlLogEntry.newBuilder
          .setOutOfTimeBoundsEntry(builder)
          .build
      }
      .orElse {
        // In case no min & max record time is set we won't be checking time bounds at post-execution
        // so the contents of this log entry does not matter.
        PartialFunction.condOpt((commitContext.minimumRecordTime, commitContext.maximumRecordTime)) {
          case (None, None) => DamlLogEntry.getDefaultInstance
        }
      }
      .getOrElse(throw new IllegalArgumentException(
        "Committer did not set an out-of-time-bounds log entry"))

  private[committer] def runSteps(
      commitContext: CommitContext,
      submission: DamlSubmission): DamlLogEntry =
    steps.foldLeft[StepResult[PartialResult]](StepContinue(init(commitContext, submission))) {
      case (state, (info, step)) =>
        state match {
          case StepContinue(state) => stepTimers(info).time(() => step(commitContext, state))
          case result @ StepStop(_) => result
        }
    } match {
      case StepStop(logEntry) => logEntry
      case _ => sys.error(s"Internal error: Committer $committerName did not produce a result!")
    }
}

object Committer {
  type StepInfo = String

  def getCurrentConfiguration(
      defaultConfig: Configuration,
      inputState: DamlStateMap,
      logger: Logger): (Option[DamlConfigurationEntry], Configuration) =
    inputState
      .getOrElse(
        Conversions.configurationStateKey,
        /* If we're retrieving configuration, we require it to at least
         * have been declared as an input by the submitter as it is used
         * to authorize configuration changes. */
        throw Err.MissingInputState(Conversions.configurationStateKey)
      )
      .flatMap { v =>
        val entry = v.getConfigurationEntry
        Configuration
          .decode(entry.getConfiguration)
          .fold({ err =>
            logger.error(s"Failed to parse configuration: $err, using default configuration.")
            None
          }, conf => Some(Some(entry) -> conf))
      }
      .getOrElse(None -> defaultConfig)

  def buildLogEntryWithOptionalRecordTime(
      recordTime: Option[Timestamp],
      addSubmissionSpecificEntry: DamlLogEntry.Builder => DamlLogEntry.Builder): DamlLogEntry = {
    val logEntryBuilder = DamlLogEntry.newBuilder
    addSubmissionSpecificEntry(logEntryBuilder)
    setRecordTimeIfAvailable(recordTime, logEntryBuilder)
    logEntryBuilder.build
  }

  private def setRecordTimeIfAvailable(
      recordTime: Option[Timestamp],
      logEntryBuilder: DamlLogEntry.Builder): DamlLogEntry.Builder =
    recordTime.fold(logEntryBuilder)(timestamp =>
      logEntryBuilder.setRecordTime(buildTimestamp(timestamp)))
}
