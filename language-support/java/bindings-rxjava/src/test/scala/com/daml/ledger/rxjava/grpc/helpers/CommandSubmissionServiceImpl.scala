// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.CommandSubmissionServiceV2Authorization
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v2.command_submission_service.{CommandSubmissionServiceGrpc, SubmitReassignmentRequest, SubmitReassignmentResponse, SubmitRequest, SubmitResponse}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class CommandSubmissionServiceImpl(getResponse: () => Future[SubmitResponse])
    extends CommandSubmissionService
    with FakeAutoCloseable {

  @volatile private var submittedRequest: Option[SubmitRequest] = None

  override def submit(request: SubmitRequest): Future[SubmitResponse] = {
    this.submittedRequest = Some(request)
    getResponse()
  }

  def getSubmittedRequest: Option[SubmitRequest] = submittedRequest

  override def submitReassignment(request: SubmitReassignmentRequest): Future[SubmitReassignmentResponse] = Future.failed(new UnsupportedOperationException())
}

object CommandSubmissionServiceImpl {

  def createWithRef(getResponse: () => Future[SubmitResponse], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, CommandSubmissionServiceImpl) = {
    val impl = new CommandSubmissionServiceImpl(getResponse)
    val authImpl = new CommandSubmissionServiceV2Authorization(impl, authorizer)
    (CommandSubmissionServiceGrpc.bindService(authImpl, ec), impl)
  }
}
