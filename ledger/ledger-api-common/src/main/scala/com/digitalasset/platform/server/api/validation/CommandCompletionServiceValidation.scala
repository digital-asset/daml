// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class CommandCompletionServiceValidation(
    val service: CommandCompletionService with AutoCloseable,
    val ledgerId: LedgerId
) extends CommandCompletionService
    with FieldValidations
    with GrpcApiService
    with ProxyCloseable
    with ErrorFactories {

  protected val logger: Logger = LoggerFactory.getLogger(CommandCompletionService.getClass)

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse]): Unit = {
    val validation = for {
      _ <- matchLedgerId(ledgerId)(request.ledgerId)
      _ <- requireNonEmptyString(request.applicationId, "application_id")
      _ <- requireNonEmpty(request.parties, "parties")
    } yield request

    validation.fold(
      exception => responseObserver.onError(exception),
      value => service.completionStream(value, responseObserver)
    )
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] = {
    matchLedgerId(ledgerId)(request.ledgerId)
      .fold(Future.failed, _ => service.completionEnd(request))
  }

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)
}
