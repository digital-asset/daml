// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.testing

import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

object DummyCommandService {

  def bind(executionContext: ExecutionContext): ServerServiceDefinition =
    CommandServiceGrpc.bindService(new DummyCommandService, executionContext)

}

final class DummyCommandService private extends CommandService {

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    Future.successful(Empty.defaultInstance)

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    Future.successful(SubmitAndWaitForTransactionIdResponse.defaultInstance)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance)

}
