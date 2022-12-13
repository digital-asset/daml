// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.StreamingServiceLifecycleManagement
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.validation.CompletionServiceRequestValidator
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.CommandCompletionService
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandCompletionService(
    service: CommandCompletionService,
    validator: CompletionServiceRequestValidator,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandCompletionServiceGrpc.CommandCompletionService
    with StreamingServiceLifecycleManagement {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  protected implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = registerStream(responseObserver) {
    validator
      .validateGrpcCompletionStreamRequest(request)
      .fold(
        t => Source.failed[CompletionStreamResponse](ValidationLogger.logFailure(request, t)),
        service.completionStreamSource,
      )
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    validator
      .validateCompletionEndRequest(request)
      .fold(
        t => Future.failed[CompletionEndResponse](ValidationLogger.logFailure(request, t)),
        _ =>
          service
            .getLedgerEnd()
            .map(abs =>
              CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value))))
            ),
      )

}
