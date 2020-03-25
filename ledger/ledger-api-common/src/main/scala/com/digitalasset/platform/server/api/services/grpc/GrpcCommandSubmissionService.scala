// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import java.time.{Duration, Instant}

import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.{
  CommandSubmissionService => ApiCommandSubmissionService
}
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest => ApiSubmitRequest
}
import com.digitalasset.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GrpcCommandSubmissionService(
    protected val service: CommandSubmissionService with AutoCloseable,
    val ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUTCTime: () => Instant,
    maxDeduplicationTime: () => Duration
) extends ApiCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(ApiCommandSubmissionService.getClass)

  private val validator =
    new SubmitRequestValidator(new CommandsValidator(ledgerId))

  override def submit(request: ApiSubmitRequest): Future[Empty] =
    validator
      .validate(request, currentLedgerTime(), currentUTCTime(), maxDeduplicationTime())
      .fold(
        Future.failed,
        service.submit(_).map(_ => Empty.defaultInstance)(DirectExecutionContext))

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, DirectExecutionContext)

}
