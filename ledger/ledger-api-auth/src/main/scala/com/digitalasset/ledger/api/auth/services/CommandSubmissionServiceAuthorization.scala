// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

final class CommandSubmissionServiceAuthorization(
    protected val service: CommandSubmissionService with AutoCloseable,
    private val authorizer: Authorizer)
    extends CommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  override def submit(request: SubmitRequest): Future[Empty] =
    authorizer.requireActClaimsForParty(request.commands.map(_.party), service.submit)(request)

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()

}
