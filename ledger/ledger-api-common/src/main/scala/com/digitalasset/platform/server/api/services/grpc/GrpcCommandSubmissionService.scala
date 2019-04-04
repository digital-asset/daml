// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.{
  CommandSubmissionService => ApiCommandSubmissionService
}
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest => ApiSubmitRequest
}
import com.digitalasset.ledger.api.validation.CommandSubmissionRequestValidator
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.common.util.DirectExecutionContext.implicitEC
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GrpcCommandSubmissionService(
    protected val service: CommandSubmissionService with AutoCloseable,
    val ledgerId: String,
    identifierResolver: IdentifierResolver)
    extends ApiCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(ApiCommandSubmissionService.getClass)

  private val validator = new CommandSubmissionRequestValidator(ledgerId, identifierResolver)

  override def submit(request: ApiSubmitRequest): Future[Empty] =
    validator
      .validate(request)
      .fold(Future.failed, req => service.submit(req).map(_ => Empty.defaultInstance))

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, DirectExecutionContext)

}
