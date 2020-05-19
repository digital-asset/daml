// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.{
  CommandSubmissionService => ApiCommandSubmissionService
}
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest => ApiSubmitRequest
}
import com.daml.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GrpcCommandSubmissionService(
    override protected val service: CommandSubmissionService with AutoCloseable,
    ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationTime: () => Option[Duration],
    metrics: Metrics,
) extends ApiCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(ApiCommandSubmissionService.getClass)

  private val validator = new SubmitRequestValidator(new CommandsValidator(ledgerId))

  override def submit(request: ApiSubmitRequest): Future[Empty] =
    Timed.future(
      metrics.daml.commands.submissions,
      Timed
        .value(
          metrics.daml.commands.validation,
          validator
            .validate(request, currentLedgerTime(), currentUtcTime(), maxDeduplicationTime()))
        .fold(
          Future.failed,
          service.submit(_).map(_ => Empty.defaultInstance)(DirectExecutionContext))
    )

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, DirectExecutionContext)

}
