// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.testing

import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

object DummyCommandSubmissionService {

  def bind(executionContext: ExecutionContext): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(new DummyCommandSubmissionService, executionContext)

}

final class DummyCommandSubmissionService private extends CommandSubmissionService {

  override def submit(request: SubmitRequest): Future[Empty] =
    Future.successful(Empty.defaultInstance)

}
