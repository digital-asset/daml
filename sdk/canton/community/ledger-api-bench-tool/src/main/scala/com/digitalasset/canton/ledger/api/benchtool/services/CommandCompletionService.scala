// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class CommandCompletionService(
    channel: Channel,
    userId: String,
    authorizationToken: Option[String],
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: CommandCompletionServiceGrpc.CommandCompletionServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      CommandCompletionServiceGrpc.stub(channel)
    )

  def completions[Result](
      config: WorkflowConfig.StreamConfig.CompletionsStreamConfig,
      observer: ObserverWithResult[CompletionStreamResponse, Result],
  ): Future[Result] = {
    val request = completionsRequest(config)
    service.completionStream(request, observer)
    logger.info(s"Started fetching completions")
    observer.result
  }

  private def completionsRequest(
      config: WorkflowConfig.StreamConfig.CompletionsStreamConfig
  ): CompletionStreamRequest = {
    if (authorizationToken.isDefined) {
      assert(
        userId == config.userId,
        s"When using user based authorization userId (${config.userId}) must be equal to userId ($userId)",
      )
    }
    val request = CompletionStreamRequest.defaultInstance
      .withParties(config.parties)
      .withUserId(config.userId)

    config.beginOffsetExclusive match {
      case Some(offset) => request.withBeginExclusive(offset)
      case None => request
    }
  }

}
