// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.Conversions.partyAllocationDedupKey
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer.{
  StepInfo,
  buildLogEntryWithOptionalRecordTime
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics

private[kvutils] class PartyAllocationCommitter(
    override protected val metrics: Metrics,
) extends Committer[DamlPartyAllocationEntry.Builder] {

  override protected val committerName = "party_allocation"

  private def rejectionTraceLog(
      msg: String,
      partyAllocationEntry: DamlPartyAllocationEntry.Builder,
  ): Unit =
    logger.trace(
      s"Party allocation rejected, $msg, correlationId=${partyAllocationEntry.getSubmissionId}")

  private val authorizeSubmission: Step = (ctx, partyAllocationEntry) => {
    if (ctx.participantId == partyAllocationEntry.getParticipantId) {
      StepContinue(partyAllocationEntry)
    } else {
      val msg =
        s"participant id ${partyAllocationEntry.getParticipantId} did not match authenticated participant id ${ctx.participantId}"
      rejectionTraceLog(msg, partyAllocationEntry)
      reject(
        ctx.recordTime,
        partyAllocationEntry,
        _.setParticipantNotAuthorized(
          ParticipantNotAuthorized.newBuilder
            .setDetails(msg)
        )
      )
    }
  }

  private val validateParty: Step = (ctx, partyAllocationEntry) => {
    val party = partyAllocationEntry.getParty
    if (Ref.Party.fromString(party).isRight) {
      StepContinue(partyAllocationEntry)
    } else {
      val msg = s"party string '$party' invalid"
      rejectionTraceLog(msg, partyAllocationEntry)
      reject(
        ctx.recordTime,
        partyAllocationEntry,
        _.setInvalidName(
          Invalid.newBuilder
            .setDetails(msg)
        )
      )
    }
  }

  private val deduplicateParty: Step = (ctx, partyAllocationEntry) => {
    val party = partyAllocationEntry.getParty
    val partyKey = DamlStateKey.newBuilder.setParty(party).build
    if (ctx.get(partyKey).isEmpty) {
      StepContinue(partyAllocationEntry)
    } else {
      val msg = s"party already exists party='$party'"
      rejectionTraceLog(msg, partyAllocationEntry)
      reject(
        ctx.recordTime,
        partyAllocationEntry,
        _.setAlreadyExists(AlreadyExists.newBuilder.setDetails(msg))
      )
    }
  }

  private val deduplicateSubmission: Step = (ctx, partyAllocationEntry) => {
    val submissionKey =
      partyAllocationDedupKey(ctx.participantId, partyAllocationEntry.getSubmissionId)
    if (ctx.get(submissionKey).isEmpty) {
      StepContinue(partyAllocationEntry)
    } else {
      val msg = s"duplicate submission='${partyAllocationEntry.getSubmissionId}'"
      rejectionTraceLog(msg, partyAllocationEntry)
      reject(
        ctx.recordTime,
        partyAllocationEntry,
        _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(msg))
      )
    }
  }

  private[committer] val buildLogEntry: Step = (ctx, partyAllocationEntry) => {
    val party = partyAllocationEntry.getParty
    val partyKey = DamlStateKey.newBuilder.setParty(party).build

    metrics.daml.kvutils.committer.partyAllocation.accepts.inc()
    logger.trace(
      s"Party allocated, party=$party correlationId=${partyAllocationEntry.getSubmissionId}")

    ctx.set(
      partyKey,
      DamlStateValue.newBuilder
        .setParty(
          DamlPartyAllocation.newBuilder
            .setParticipantId(ctx.participantId)
        )
        .build
    )

    ctx.set(
      partyAllocationDedupKey(ctx.participantId, partyAllocationEntry.getSubmissionId),
      DamlStateValue.newBuilder
        .setSubmissionDedup(DamlSubmissionDedupValue.newBuilder)
        .build
    )

    val successLogEntry = buildLogEntryWithOptionalRecordTime(
      ctx.recordTime,
      _.setPartyAllocationEntry(partyAllocationEntry))
    if (ctx.preExecute) {
      setOutOfTimeBoundsLogEntry(partyAllocationEntry, ctx)
    }
    StepStop(successLogEntry)
  }

  private def reject[PartialResult](
      recordTime: Option[Timestamp],
      partyAllocationEntry: DamlPartyAllocationEntry.Builder,
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder,
  ): StepResult[PartialResult] = {
    metrics.daml.kvutils.committer.partyAllocation.rejections.inc()
    StepStop(buildRejectionLogEntry(recordTime, partyAllocationEntry, addErrorDetails))
  }

  private def buildRejectionLogEntry(
      recordTime: Option[Timestamp],
      partyAllocationEntry: DamlPartyAllocationEntry.Builder,
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder,
  ): DamlLogEntry = {
    buildLogEntryWithOptionalRecordTime(
      recordTime,
      _.setPartyAllocationRejectionEntry(
        addErrorDetails(
          DamlPartyAllocationRejectionEntry.newBuilder
            .setSubmissionId(partyAllocationEntry.getSubmissionId)
            .setParticipantId(partyAllocationEntry.getParticipantId)
        )
      )
    )
  }

  private def setOutOfTimeBoundsLogEntry(
      partyAllocationEntry: DamlPartyAllocationEntry.Builder,
      commitContext: CommitContext): Unit = {
    commitContext.outOfTimeBoundsLogEntry = Some(
      buildRejectionLogEntry(recordTime = None, partyAllocationEntry, identity)
    )
  }

  override protected def init(
      ctx: CommitContext,
      submission: DamlSubmission,
  ): DamlPartyAllocationEntry.Builder =
    submission.getPartyAllocationEntry.toBuilder

  override protected val steps: Iterable[(StepInfo, Step)] = Iterable(
    "authorize_submission" -> authorizeSubmission,
    "validate_party" -> validateParty,
    "deduplicate_submission" -> deduplicateSubmission,
    "deduplicate_party" -> deduplicateParty,
    "build_log_entry" -> buildLogEntry
  )

}
