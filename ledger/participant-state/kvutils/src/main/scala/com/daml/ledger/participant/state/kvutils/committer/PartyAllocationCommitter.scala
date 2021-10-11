// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.Conversions.partyAllocationDedupKey
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer.buildLogEntryWithOptionalRecordTime
import com.daml.ledger.participant.state.kvutils.store.{
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue,
  DamlSubmissionDedupValue,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.entries.LoggingEntries
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

private[kvutils] object PartyAllocationCommitter {
  type Result = DamlPartyAllocationEntry.Builder

  type Step = CommitStep[Result]
}

private[kvutils] class PartyAllocationCommitter(
    override protected val metrics: Metrics
) extends Committer[PartyAllocationCommitter.Result] {

  import PartyAllocationCommitter._

  private final val logger = ContextualizedLogger.get(getClass)

  override protected val committerName = "party_allocation"

  override protected def extraLoggingContext(result: Result): LoggingEntries =
    LoggingEntries("party" -> result.getParty)

  override protected def init(
      ctx: CommitContext,
      submission: DamlSubmission,
  )(implicit loggingContext: LoggingContext): Result =
    submission.getPartyAllocationEntry.toBuilder

  private def rejectionTraceLog(msg: String)(implicit loggingContext: LoggingContext): Unit =
    logger.trace(s"Party allocation rejected: $msg.")

  private val authorizeSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] =
      if (ctx.participantId == result.getParticipantId) {
        StepContinue(result)
      } else {
        val message =
          s"participant id ${result.getParticipantId} did not match authenticated participant id ${ctx.participantId}"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder.setDetails(message)),
        )
      }
  }

  private val validateParty: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val party = result.getParty
      if (Ref.Party.fromString(party).isRight) {
        StepContinue(result)
      } else {
        val message = s"party string '$party' invalid"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result,
          _.setInvalidName(Invalid.newBuilder.setDetails(message)),
        )
      }
    }
  }

  private val deduplicateParty: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val party = result.getParty
      val partyKey = DamlStateKey.newBuilder.setParty(party).build
      if (ctx.get(partyKey).isEmpty) {
        StepContinue(result)
      } else {
        val message = s"party already exists party='$party'"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result,
          _.setAlreadyExists(AlreadyExists.newBuilder.setDetails(message)),
        )
      }
    }
  }

  private val deduplicateSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val submissionKey = partyAllocationDedupKey(ctx.participantId, result.getSubmissionId)
      if (ctx.get(submissionKey).isEmpty) {
        StepContinue(result)
      } else {
        val message = s"duplicate submission='${result.getSubmissionId}'"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(message)),
        )
      }
    }
  }

  private[committer] val buildLogEntry: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val party = result.getParty
      val partyKey = DamlStateKey.newBuilder.setParty(party).build

      metrics.daml.kvutils.committer.partyAllocation.accepts.inc()
      logger.trace("Party allocated.")

      ctx.set(
        partyKey,
        DamlStateValue.newBuilder
          .setParty(
            DamlPartyAllocation.newBuilder
              .setParticipantId(ctx.participantId)
          )
          .build,
      )

      ctx.set(
        partyAllocationDedupKey(ctx.participantId, result.getSubmissionId),
        DamlStateValue.newBuilder
          .setSubmissionDedup(DamlSubmissionDedupValue.newBuilder)
          .build,
      )

      val successLogEntry = buildLogEntryWithOptionalRecordTime(
        ctx.recordTime,
        _.setPartyAllocationEntry(result),
      )
      if (ctx.preExecute) {
        setOutOfTimeBoundsLogEntry(result, ctx)
      }
      StepStop(successLogEntry)
    }
  }

  private def reject[PartialResult](
      recordTime: Option[Timestamp],
      result: Result,
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder,
  ): StepResult[PartialResult] = {
    metrics.daml.kvutils.committer.partyAllocation.rejections.inc()
    StepStop(buildRejectionLogEntry(recordTime, result, addErrorDetails))
  }

  private def buildRejectionLogEntry(
      recordTime: Option[Timestamp],
      result: Result,
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder,
  ): DamlLogEntry = {
    buildLogEntryWithOptionalRecordTime(
      recordTime,
      _.setPartyAllocationRejectionEntry(
        addErrorDetails(
          DamlPartyAllocationRejectionEntry.newBuilder
            .setSubmissionId(result.getSubmissionId)
            .setParticipantId(result.getParticipantId)
        )
      ),
    )
  }

  private def setOutOfTimeBoundsLogEntry(result: Result, commitContext: CommitContext): Unit = {
    commitContext.outOfTimeBoundsLogEntry = Some(
      buildRejectionLogEntry(recordTime = None, result, identity)
    )
  }

  override protected val steps: Steps[Result] = Iterable(
    "authorize_submission" -> authorizeSubmission,
    "validate_party" -> validateParty,
    "deduplicate_submission" -> deduplicateSubmission,
    "deduplicate_party" -> deduplicateParty,
    "build_log_entry" -> buildLogEntry,
  )
}
