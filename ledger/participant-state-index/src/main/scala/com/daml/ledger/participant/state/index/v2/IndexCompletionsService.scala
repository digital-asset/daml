// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.{ApplicationId, LedgerOffset}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse

/**
  * Serves as a backend to implement
  * [[com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService]]
  **/
trait IndexCompletionsService extends LedgerEndService {
  def getCompletions(
      begin: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]
  ): Source[CompletionStreamResponse, NotUsed]
}
