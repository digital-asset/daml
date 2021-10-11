// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse

import scala.concurrent.Future

trait CommandCompletionService {

  def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute]

  def completionStreamSource(
      request: CompletionStreamRequest
  ): Source[CompletionStreamResponse, NotUsed]

}
