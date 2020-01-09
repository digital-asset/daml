// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.Counter
import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildTimestamp,
  partyAllocationDedupKey
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.digitalasset.daml.lf.data.Ref

private[kvutils] case object PartyAllocationCommitter
    extends Committer[DamlPartyAllocationEntry, DamlPartyAllocationEntry.Builder] {

  private object Metrics {
    // kvutils.PartyAllocationCommitter.*
    val accepts: Counter = metricsRegistry.counter(metricsName("accepts"))
    val rejections: Counter = metricsRegistry.counter(metricsName("rejections"))
  }

  private def rejectionTraceLog(
      msg: String,
      partyAllocationEntry: DamlPartyAllocationEntry.Builder): Unit =
    logger.trace(
      s"Party allocation rejected, $msg, correlationId=${partyAllocationEntry.getSubmissionId}")

  private val authorizeSubmission: Step = (ctx, partyAllocationEntry) => {
    if (ctx.getParticipantId == partyAllocationEntry.getParticipantId)
      StepContinue(partyAllocationEntry)
    else {
      val msg =
        s"participant id ${partyAllocationEntry.getParticipantId} did not match authenticated participant id ${ctx.getParticipantId}"
      rejectionTraceLog(msg, partyAllocationEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          partyAllocationEntry,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder
            .setDetails(msg))))
    }
  }

  private val validateParty: Step = (ctx, partyAllocationEntry) => {
    val party = partyAllocationEntry.getParty
    if (Ref.Party.fromString(party).isRight)
      StepContinue(partyAllocationEntry)
    else {
      val msg = s"party string '${party}' invalid"
      rejectionTraceLog(msg, partyAllocationEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          partyAllocationEntry,
          _.setInvalidName(Invalid.newBuilder
            .setDetails(msg))))
    }
  }

  private val deduplicateParty: Step = (ctx, partyAllocationEntry) => {
    val party = partyAllocationEntry.getParty
    val partyKey = DamlStateKey.newBuilder.setParty(party).build
    if (ctx.get(partyKey).isEmpty)
      StepContinue(partyAllocationEntry)
    else {
      val msg = s"party already exists party='$party'"
      rejectionTraceLog(msg, partyAllocationEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          partyAllocationEntry,
          _.setAlreadyExists(AlreadyExists.newBuilder.setDetails(msg))
        )
      )
    }
  }

  private val deduplicateSubmission: Step = (ctx, partyAllocationEntry) => {
    val submissionKey =
      partyAllocationDedupKey(ctx.getParticipantId, partyAllocationEntry.getSubmissionId)
    if (ctx.get(submissionKey).isEmpty)
      StepContinue(partyAllocationEntry)
    else {
      val msg = s"duplicate submission='${partyAllocationEntry.getSubmissionId}'"
      rejectionTraceLog(msg, partyAllocationEntry)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          partyAllocationEntry,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(msg))
        )
      )
    }
  }

  private val buildLogEntry: Step = (ctx, partyAllocationEntry) => {
    val party = partyAllocationEntry.getParty
    val partyKey = DamlStateKey.newBuilder.setParty(party).build

    Metrics.accepts.inc()
    logger.trace(
      s"Party allocated, party=$party correlationId=${partyAllocationEntry.getSubmissionId}")

    ctx.set(
      partyKey,
      DamlStateValue.newBuilder
        .setParty(
          DamlPartyAllocation.newBuilder
            .setParticipantId(ctx.getParticipantId)
        )
        .build
    )

    ctx.set(
      partyAllocationDedupKey(ctx.getParticipantId, partyAllocationEntry.getSubmissionId),
      DamlStateValue.newBuilder
        .setSubmissionDedup(
          DamlSubmissionDedupValue.newBuilder
            .setRecordTime(buildTimestamp(ctx.getRecordTime))
            .build)
        .build
    )

    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setPartyAllocationEntry(partyAllocationEntry)
        .build
    )
  }

  private def buildRejectionLogEntry(
      ctx: CommitContext,
      partyAllocationEntry: DamlPartyAllocationEntry.Builder,
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder)
    : DamlLogEntry = {
    Metrics.rejections.inc()
    DamlLogEntry.newBuilder
      .setRecordTime(buildTimestamp(ctx.getRecordTime))
      .setPartyAllocationRejectionEntry(
        addErrorDetails(
          DamlPartyAllocationRejectionEntry.newBuilder
            .setSubmissionId(partyAllocationEntry.getSubmissionId)
            .setParticipantId(partyAllocationEntry.getParticipantId)
        )
      )
      .build
  }

  override def init(
      ctx: CommitContext,
      partyAllocationEntry: DamlPartyAllocationEntry): DamlPartyAllocationEntry.Builder =
    partyAllocationEntry.toBuilder

  override val steps: Iterable[(StepInfo, Step)] = Iterable(
    "authorize_submission" -> authorizeSubmission,
    "validate_party" -> validateParty,
    "deduplicate_submission" -> deduplicateSubmission,
    "deduplicate_party" -> deduplicateParty,
    "build_log_entry" -> buildLogEntry
  )

  override lazy val committerName = "party_allocation"

}
