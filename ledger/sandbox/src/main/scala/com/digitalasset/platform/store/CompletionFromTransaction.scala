// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.ledger.participant.state.v1.{Offset, RejectionReason}
import com.daml.api.util.TimestampConversion.fromInstant
import com.daml.lf.data.Ref
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.command_completion_service.{Checkpoint, CompletionStreamResponse}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.store.entries.LedgerEntry
import com.google.rpc.status.Status
import io.grpc.Status.Code

// Turn a stream of transactions into a stream of completions for a given application and set of parties
// TODO Restrict the scope of this to com.daml.platform.store.dao when
// TODO - the in-memory sandbox is gone
private[platform] object CompletionFromTransaction {

  def toApiCheckpoint(recordTime: Instant, offset: Offset): Some[Checkpoint] =
    Some(
      Checkpoint(
        recordTime = Some(fromInstant(recordTime)),
        offset = Some(LedgerOffset(LedgerOffset.Value.Absolute(offset.toApiString)))))

  // We _rely_ on the following compiler flags for this to be safe:
  // * -Xno-patmat-analysis _MUST NOT_ be enabled
  // * -Xfatal-warnings _MUST_ be enabled
  def toErrorCode(rejection: RejectionReason): Code = {
    rejection match {
      case _: RejectionReason.Inconsistent | _: RejectionReason.Disputed |
          _: RejectionReason.PartyNotKnownOnLedger =>
        Code.INVALID_ARGUMENT
      case _: RejectionReason.ResourcesExhausted | _: RejectionReason.InvalidLedgerTime =>
        Code.ABORTED
      case _: RejectionReason.SubmitterCannotActViaParticipant =>
        Code.PERMISSION_DENIED
    }
  }

  private def toParticipantRejection(reason: domain.RejectionReason): RejectionReason =
    reason match {
      case r: domain.RejectionReason.Inconsistent =>
        RejectionReason.Inconsistent(r.description)
      case r: domain.RejectionReason.Disputed =>
        RejectionReason.Disputed(r.description)
      case r: domain.RejectionReason.OutOfQuota =>
        RejectionReason.ResourcesExhausted(r.description)
      case r: domain.RejectionReason.PartyNotKnownOnLedger =>
        RejectionReason.PartyNotKnownOnLedger(r.description)
      case r: domain.RejectionReason.SubmitterCannotActViaParticipant =>
        RejectionReason.SubmitterCannotActViaParticipant(r.description)
      case r: domain.RejectionReason.InvalidLedgerTime =>
        RejectionReason.InvalidLedgerTime(r.description)
    }

  // Filter completions for transactions for which we have the full submitter information: appId, submitter, cmdId
  // This doesn't make a difference for the sandbox (because it represents the ledger backend + api server in single package).
  // But for an api server that is part of a distributed ledger network, we might see
  // transactions that originated from some other api server. These transactions don't contain the submitter information,
  // and therefore we don't emit CommandAccepted completions for those
  def apply(appId: ApplicationId, parties: Set[Ref.Party])
    : PartialFunction[(Offset, LedgerEntry), (Offset, CompletionStreamResponse)] = {
    case (
        offset,
        LedgerEntry.Transaction(
          Some(commandId),
          transactionId,
          Some(`appId`),
          Some(submitter),
          _,
          _,
          recordTime,
          _,
          _)) if parties(submitter) =>
      offset -> CompletionStreamResponse(
        checkpoint = toApiCheckpoint(recordTime, offset),
        Seq(Completion(commandId, Some(Status()), transactionId))
      )
    case (offset, LedgerEntry.Rejection(recordTime, commandId, `appId`, submitter, reason))
        if parties(submitter) =>
      offset -> CompletionStreamResponse(
        checkpoint = toApiCheckpoint(recordTime, offset),
        Seq(
          Completion(
            commandId,
            Some(Status(toErrorCode(toParticipantRejection(reason)).value(), reason.description))
          )
        )
      )
  }

}
