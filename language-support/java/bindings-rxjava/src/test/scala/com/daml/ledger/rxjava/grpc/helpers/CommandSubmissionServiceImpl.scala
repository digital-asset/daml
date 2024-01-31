// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.CommandSubmissionServiceAuthorization
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class CommandSubmissionServiceImpl(getResponse: () => Future[Empty])
    extends CommandSubmissionService
    with FakeAutoCloseable {

  @volatile private var submittedRequest: Option[SubmitRequest] = None

  override def submit(request: SubmitRequest): Future[Empty] = {
    this.submittedRequest = Some(request)
    getResponse()
  }

  def getSubmittedRequest: Option[SubmitRequest] = submittedRequest
}

object CommandSubmissionServiceImpl {

  def createWithRef(getResponse: () => Future[Empty], authorizer: Authorizer)(implicit
      ec: ExecutionContext
  ): (ServerServiceDefinition, CommandSubmissionServiceImpl) = {
    val impl = new CommandSubmissionServiceImpl(getResponse)
    val authImpl = new CommandSubmissionServiceAuthorization(impl, authorizer)
    (CommandSubmissionServiceGrpc.bindService(authImpl, ec), impl)
  }
}
