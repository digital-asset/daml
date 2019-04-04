// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class CommandSubmissionServiceImpl(response: Future[Empty]) extends CommandSubmissionService {

  private var submittedRequest: Option[SubmitRequest] = None

  override def submit(request: SubmitRequest): Future[Empty] = {
    this.submittedRequest = Some(request)
    response
  }

  def getSubmittedRequest: Option[SubmitRequest] = submittedRequest
}

object CommandSubmissionServiceImpl {

  def createWithRef(response: Future[Empty])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, CommandSubmissionServiceImpl) = {
    val serviceImpl = new CommandSubmissionServiceImpl(response)
    (CommandSubmissionServiceGrpc.bindService(serviceImpl, ec), serviceImpl)
  }
}
