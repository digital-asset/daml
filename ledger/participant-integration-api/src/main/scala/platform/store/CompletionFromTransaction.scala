// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.api.util.TimestampConversion.fromInstant
import com.daml.ledger.api.v1.command_completion_service.{Checkpoint, CompletionStreamResponse}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.store.Conversions.RejectionReasonOps
import com.daml.platform.store.entries.LedgerEntry
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status

// Turn a stream of transactions into a stream of completions for a given application and set of parties
// TODO Restrict the scope of this to com.daml.platform.store.dao when
// TODO - the in-memory sandbox is gone
private[platform] object CompletionFromTransaction {
  private val OkStatus = StatusProto.of(Status.Code.OK.value(), "", Seq.empty)
  private val RejectionTransactionId = ""

  // Filter completions for transactions for which we have the full submitter information: appId, submitter, cmdId
  // This doesn't make a difference for the sandbox (because it represents the ledger backend + api server in single package).
  // But for an api server that is part of a distributed ledger network, we might see
  // transactions that originated from some other api server. These transactions don't contain the submitter information,
  // and therefore we don't emit CommandAccepted completions for those
  def apply(
      appId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  ): PartialFunction[(Offset, LedgerEntry), (Offset, CompletionStreamResponse)] = {
    case (
          offset,
          LedgerEntry.Transaction(
            Some(commandId),
            transactionId,
            Some(`appId`),
            _,
            actAs,
            _,
            _,
            recordTime,
            _,
            _,
          ),
        ) if actAs.exists(parties) =>
      offset -> acceptedCompletion(recordTime, offset, commandId, transactionId)

    case (offset, LedgerEntry.Rejection(recordTime, commandId, `appId`, _, actAs, reason))
        if actAs.exists(parties) =>
      val status = reason.toParticipantStateRejectionReason.status
      offset -> rejectedCompletion(recordTime, offset, commandId, status)
  }

  def acceptedCompletion(
      recordTime: Instant,
      offset: Offset,
      commandId: String,
      transactionId: String,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completions = Seq(Completion.of(commandId, Some(OkStatus), transactionId)),
    )

  def rejectedCompletion(
      recordTime: Instant,
      offset: Offset,
      commandId: String,
      status: StatusProto,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completions = Seq(Completion.of(commandId, Some(status), RejectionTransactionId)),
    )

  private def toApiCheckpoint(recordTime: Instant, offset: Offset): Checkpoint =
    Checkpoint.of(
      recordTime = Some(fromInstant(recordTime)),
      offset = Some(LedgerOffset.of(LedgerOffset.Value.Absolute(offset.toApiString))),
    )
}
