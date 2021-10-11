// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.Config
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class CommandCompletionService(
    channel: Channel,
    ledgerId: String,
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: CommandCompletionServiceGrpc.CommandCompletionServiceStub =
    CommandCompletionServiceGrpc.stub(channel)

  def completions[Result](
      config: Config.StreamConfig.CompletionsStreamConfig,
      observer: ObserverWithResult[CompletionStreamResponse, Result],
  ): Future[Result] = {
    val request = completionsRequest(ledgerId, config)
    service.completionStream(request, observer)
    logger.info(s"Started fetching completions")
    observer.result
  }

  private def completionsRequest(
      ledgerId: String,
      config: Config.StreamConfig.CompletionsStreamConfig,
  ): CompletionStreamRequest = {
    val request = CompletionStreamRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withParties(List(config.party))
      .withApplicationId(config.applicationId)

    config.beginOffset match {
      case Some(offset) => request.withOffset(offset)
      case None => request
    }
  }

}
