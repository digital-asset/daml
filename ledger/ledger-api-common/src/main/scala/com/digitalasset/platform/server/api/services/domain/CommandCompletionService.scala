// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerOffset
import com.digitalasset.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse

import scala.concurrent.Future

trait CommandCompletionService {

  def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, NotUsed]

  def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute]

}
