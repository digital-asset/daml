// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.api.util.TimestampConversion.fromInstant
import com.daml.ledger.api.v1.command_completion_service.{Checkpoint, CompletionStreamResponse}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.offset.Offset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status

// Turn a stream of transactions into a stream of completions for a given application and set of parties
// TODO Restrict the scope of this to com.daml.platform.store.dao when
// TODO - the in-memory sandbox is gone
private[platform] object CompletionFromTransaction {
  private val OkStatus = StatusProto.of(Status.Code.OK.value(), "", Seq.empty)
  private val RejectionTransactionId = ""

  def acceptedCompletion(
      recordTime: Instant,
      offset: Offset,
      commandId: String,
      transactionId: String,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completions = Seq(
        Completion().update(
          _.commandId := commandId,
          _.status := OkStatus,
          _.transactionId := transactionId,
        )
      ),
    )

  def rejectedCompletion(
      recordTime: Instant,
      offset: Offset,
      commandId: String,
      status: StatusProto,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      checkpoint = Some(toApiCheckpoint(recordTime, offset)),
      completions = Seq(
        Completion().update(
          _.commandId := commandId,
          _.status := status,
          _.transactionId := RejectionTransactionId,
        )
      ),
    )

  private def toApiCheckpoint(recordTime: Instant, offset: Offset): Checkpoint =
    Checkpoint.of(
      recordTime = Some(fromInstant(recordTime)),
      offset = Some(LedgerOffset.of(LedgerOffset.Value.Absolute(offset.toApiString))),
    )
}
