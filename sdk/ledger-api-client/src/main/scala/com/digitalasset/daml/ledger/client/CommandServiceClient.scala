// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitForTransactionResponse,
}
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
}
import com.digitalasset.canton.tracing.TraceContext
import com.google.rpc.status.Status
import io.grpc.protobuf.StatusProto

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class CommandServiceClient(
    service: CommandServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    executionContext: ExecutionContext
) {

  private def handleException[R](exception: Throwable): Future[Either[Status, R]] =
    CommandServiceClient.statusFromThrowable(exception) match {
      case Some(value) => Future.successful(Left(value))
      case None => Future.failed(exception)
    }

  /** Submits and waits, optionally with a custom timeout.
    *
    * Note that the [[com.daml.ledger.api.v2.commands.Commands]] argument is scala protobuf. If you
    * use java codegen, you need to convert the List[Command] using the codegenToScalaProto method.
    */
  def submitAndWaitForTransaction(
      commands: Commands,
      transactionShape: TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
      timeout: Option[Duration] = None,
      token: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Status, SubmitAndWaitForTransactionResponse]] =
    submitAndHandle(
      timeout,
      token,
      _.submitAndWaitForTransaction(
        getSubmitAndWaitForTransactionRequest(Some(commands), transactionShape)
      ),
    )

  private def serviceWithTokenAndDeadline(
      timeout: Option[Duration],
      token: Option[String],
  )(implicit traceContext: TraceContext): CommandServiceStub = {
    val withToken: CommandServiceStub =
      LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken()))

    timeout.fold(withToken) { timeout =>
      withToken.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
  }

  private def submitAndHandle[R](
      timeout: Option[Duration],
      token: Option[String],
      request: CommandServiceStub => Future[R],
  )(implicit traceContext: TraceContext): Future[Either[Status, R]] =
    request(serviceWithTokenAndDeadline(timeout, token))
      .transformWith {
        case Success(value) => Future.successful(Right(value))
        case Failure(exception) => handleException(exception)
      }

  private def getSubmitAndWaitForTransactionRequest(
      commands: Option[Commands],
      transactionShape: TransactionShape,
  ) =
    SubmitAndWaitForTransactionRequest(
      commands = commands,
      transactionFormat = Some(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = commands.toList
                .flatMap(_.actAs)
                .map(
                  _ -> Filters(
                    cumulative = Nil
                  )
                )
                .toMap,
              filtersForAnyParty = None,
              verbose = true,
            )
          ),
          transactionShape = transactionShape,
        )
      ),
    )
}

object CommandServiceClient {
  def statusFromThrowable(throwable: Throwable): Option[Status] =
    Option(StatusProto.fromThrowable(throwable)).map(Status.fromJavaProto)
}
