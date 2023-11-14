// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait CommandCompletionService {

  def getLedgerEnd(implicit loggingContext: LoggingContextWithTrace): Future[LedgerOffset.Absolute]

  def completionStreamSource(
      request: CompletionStreamRequest
  )(implicit loggingContext: LoggingContextWithTrace): Source[CompletionStreamResponse, NotUsed]

}
