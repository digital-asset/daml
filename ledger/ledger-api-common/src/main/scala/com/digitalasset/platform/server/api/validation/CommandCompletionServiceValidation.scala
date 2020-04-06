// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.dec.DirectExecutionContext
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

//TODO: this class is only needed by DamlOnXCommandCompletionService.scala. Must be deleted once that's gone!
class CommandCompletionServiceValidation(
    val service: CommandCompletionService with AutoCloseable,
    val ledgerId: LedgerId)
    extends CommandCompletionService
    with FieldValidations
    with GrpcApiService
    with ProxyCloseable
    with ErrorFactories {

  protected val logger: Logger = LoggerFactory.getLogger(CommandCompletionService.getClass)

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse]): Unit = {
    val validation = for {
      _ <- matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      _ <- requireNonEmptyString(request.applicationId, "application_id")
      _ <- requireNonEmpty(request.parties, "parties")
    } yield request

    validation.fold(
      exception => responseObserver.onError(exception),
      value => service.completionStream(value, responseObserver)
    )
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] = {
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .fold(Future.failed, _ => service.completionEnd(request))
  }

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)
}
