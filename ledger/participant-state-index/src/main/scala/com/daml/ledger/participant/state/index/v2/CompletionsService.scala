// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerOffset}

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService]]
  **/
trait CompletionsService {
  def getCompletions(
      begin: Option[LedgerOffset],
      applicationId: ApplicationId,
      parties: List[Ref.Party]
  ): Source[CompletionEvent, NotUsed]
}
