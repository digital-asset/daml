// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{CompletionEvent, LedgerOffset}
import com.digitalasset.ledger.api.messages.command.completion.CompletionStreamRequest

import scala.concurrent.Future

trait CommandCompletionService {

  def completionStreamSource(request: CompletionStreamRequest): Source[CompletionEvent, NotUsed]

  def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute]

}
