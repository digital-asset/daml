// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService]]
  */
trait IndexCompletionsService extends LedgerEndService {
  def getCompletions(
      begin: LedgerOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed]

  // TODO Remove, as possible. This is solely serving KV Deduplication Offset -> Duration conversion
  def getCompletions(
      startExclusive: LedgerOffset,
      endInclusive: LedgerOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed]
}
