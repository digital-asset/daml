// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitForUpdateIdResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.commands.Commands
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient.{
  statusFromThrowable,
  withDeadline,
}
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status
import io.grpc.protobuf.StatusProto
import io.grpc.stub.AbstractStub

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandServiceClient(service: CommandServiceStub)(implicit
    executionContext: ExecutionContext
) {

  def submitAndWait(commands: Commands): Future[Either[Status, Unit]] = {
    submitAndWait(SubmitAndWaitRequest(commands = Some(commands)))
      .transformWith {
        case Success(_) => Future.successful(Right(()))
        case Failure(exception) =>
          statusFromThrowable(exception) match {
            case Some(value) => Future.successful(Left(value))
            case None => Future.failed(exception)
          }
      }
  }

  def submitAndWait(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[Empty] =
    withDeadline(LedgerClient.stub(service, token), timeout).submitAndWait(submitAndWaitRequest)

  def submitAndWaitForUpdateId(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[SubmitAndWaitForUpdateIdResponse] =
    withDeadline(LedgerClient.stub(service, token), timeout)
      .submitAndWaitForUpdateId(submitAndWaitRequest)

  def submitAndWaitForTransaction(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[SubmitAndWaitForTransactionResponse] =
    withDeadline(LedgerClient.stub(service, token), timeout)
      .submitAndWaitForTransaction(submitAndWaitRequest)

  def submitAndWaitForTransactionTree(
      submitAndWaitRequest: SubmitAndWaitRequest,
      token: Option[String] = None,
      timeout: Option[Duration] = None,
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    withDeadline(LedgerClient.stub(service, token), timeout)
      .submitAndWaitForTransactionTree(submitAndWaitRequest)

}

object CommandServiceClient {
  private def statusFromThrowable(throwable: Throwable): Option[Status] =
    Option(StatusProto.fromThrowable(throwable)).map(Status.fromJavaProto)

  private def withDeadline[S <: AbstractStub[S]](stub: S, timeout: Option[Duration]): S =
    timeout.fold(stub)(timeout => stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS))

}
