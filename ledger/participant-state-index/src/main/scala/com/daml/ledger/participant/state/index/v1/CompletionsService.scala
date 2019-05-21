// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{ApplicationId, Offset, Party}

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService]]
  **/
trait CompletionsService {
  def getCompletions(
      beginAfter: Option[Offset],
      applicationId: ApplicationId,
      parties: List[Party]
  ): Source[CompletionEvent, NotUsed]
}
