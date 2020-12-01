// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service._
import com.daml.ledger.api.validation.CommandsValidator
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

private[daml] final class CommandSubmissionServiceAuthorization(
    protected val service: CommandSubmissionService with AutoCloseable,
    private val authorizer: Authorizer)
    extends CommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  override def submit(request: SubmitRequest): Future[Empty] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationId = request.commands.map(_.applicationId),
      call = service.submit,
    )(request)
  }

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

}
