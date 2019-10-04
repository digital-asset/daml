package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics
import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.daml.lf.data.Time
import org.slf4j.{Logger, LoggerFactory}

trait Committer[Submission, PartialResult] {

  /** A kvutils committer is composed of individual commit steps which:
    *  - carry a partial result from one step to another
    *  - can access daml state via [[Context.get]]
    *  - can update daml state via [[Context.set]]
    *  - can end the commit via [[Context.done]], which skips the rest of the remaining steps
    */
  type Step = (Context, PartialResult) => PartialResult
  def steps: Iterable[Step]

  /** The initial partial result passed to first step. */
  def init(subm: Submission): PartialResult

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val metricsRegistry: metrics.MetricRegistry =
    metrics.SharedMetricRegistries.getOrCreate("kvutils")
  def metricsName(metric: String): String = metrics.MetricRegistry.name(this.getClass, metric)
  private val runTimer: Timer = metricsRegistry.timer(metricsName("run-timer"))

  /** A committer can `run` a submission and produce a log entry and output states. */
  def run(
      entryId: DamlLogEntryId,
      recordTime: Time.Timestamp,
      submission: Submission,
      participantId: ParticipantId,
      inputState: DamlStateMap): (DamlLogEntry, Iterable[(DamlStateKey, DamlStateValue)]) =
    runTimer.time { () =>
      var result: Option[DamlLogEntry] = None
      val ctx = new Context {
        override def getRecordTime: Time.Timestamp = recordTime
        override def getParticipantId: ParticipantId = participantId
        override def inputs: DamlStateMap = inputState
        override def done[Void](logEntry: DamlLogEntry): Void = {
          result = Some(logEntry)
          // Return "null", giving us the assertion and a compiler warning if the committer
          // implementation tries to do anything following a `done` in the same function.
          null.asInstanceOf[Void]
        }
      }

      val stepIter = steps.iterator
      var partialResult = init(submission)

      while (result.isEmpty && stepIter.hasNext) {
        partialResult = stepIter.next()(ctx, partialResult)
      }
      assert(result.isDefined, s"${this.getClass} did not produce a log entry!")

      result.get -> ctx.getOutputs
    }
}
