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
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient.statusFromThrowable
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.rpc.status.Status
import io.grpc.protobuf.StatusProto

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandServiceClient(service: CommandServiceStub)(implicit
    executionContext: ExecutionContext
) {

  private def handleException[R](exception: Throwable): Future[Either[Status, R]] = {
    statusFromThrowable(exception) match {
      case Some(value) => Future.successful(Left(value))
      case None => Future.failed(exception)
    }
  }

  /** Submits and waits, optionally with a custom timeout
    *
    * Note that the [[com.daml.ledger.api.v2.commands.Commands]] argument is scala protobuf. If you use java codegen,
    * you need to convert the List[Command] using the codegenToScalaProto method
    */
  def submitAndWait(
      commands: Commands,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[Either[Status, Unit]] =
    submitAndHandle(
      timeout,
      token,
      _.submitAndWait(SubmitAndWaitRequest(commands = Some(commands))).map(_ => ()),
    )

  def deprecatedSubmitAndWaitForTransactionForJsonApi(
      request: SubmitAndWaitRequest,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[SubmitAndWaitForTransactionResponse] =
    serviceWithTokenAndDeadline(timeout, token).submitAndWaitForTransaction(
      request
    )

  def deprecatedSubmitAndWaitForTransactionTreeForJsonApi(
      request: SubmitAndWaitRequest,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[SubmitAndWaitForTransactionTreeResponse] =
    serviceWithTokenAndDeadline(timeout, token).submitAndWaitForTransactionTree(
      request
    )

  def submitAndWaitForTransaction(
      commands: Commands,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Status, SubmitAndWaitForTransactionResponse]] =
    submitAndHandle(
      timeout,
      token,
      _.submitAndWaitForTransaction(SubmitAndWaitRequest(commands = Some(commands))),
    )

  def submitAndWaitForTransactionTree(
      commands: Commands,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Status, SubmitAndWaitForTransactionTreeResponse]] =
    submitAndHandle(
      timeout,
      token,
      _.submitAndWaitForTransactionTree(SubmitAndWaitRequest(commands = Some(commands))),
    )

  def submitAndWaitForUpdateId(
      commands: Commands,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Status, SubmitAndWaitForUpdateIdResponse]] = {
    submitAndHandle(
      timeout,
      token,
      _.submitAndWaitForUpdateId(SubmitAndWaitRequest(commands = Some(commands))),
    )
  }

  private def serviceWithTokenAndDeadline(
      timeout: Option[Duration],
      token: Option[String],
  )(implicit traceContext: TraceContext): CommandServiceStub = {
    val withToken: CommandServiceStub = LedgerClient
      .stub(service, token)
      .withOption(TraceContextGrpc.TraceContextOptionsKey, traceContext)

    timeout
      .fold(withToken) { timeout =>
        withToken
          .withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS)
      }
  }

  private def submitAndHandle[R](
      timeout: Option[Duration],
      token: Option[String],
      request: CommandServiceStub => Future[R],
  )(implicit traceContext: TraceContext): Future[Either[Status, R]] = {
    request(serviceWithTokenAndDeadline(timeout, token))
      .transformWith {
        case Success(value) => Future.successful(Right(value))
        case Failure(exception) => handleException(exception)
      }
  }
}

object CommandServiceClient {
  def statusFromThrowable(throwable: Throwable): Option[Status] =
    Option(StatusProto.fromThrowable(throwable)).map(Status.fromJavaProto)

}
