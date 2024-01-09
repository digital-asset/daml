// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import cats.data.EitherT
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.ledger.api.auth.client.LedgerCallCredentials
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.LoggerUtil
import io.grpc.ManagedChannel
import io.grpc.stub.AbstractStub

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Run a command using the default workflow
  */
class GrpcCtlRunner(
    maxRequestDebugLines: Int,
    maxRequestDebugStringLength: Int,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Runs a command
    * @return Either a printable error as a String or a Unit indicating all was successful
    */
  def run[Req, Res, Result](
      instanceName: String,
      command: GrpcAdminCommand[Req, Res, Result],
      channel: ManagedChannel,
      token: Option[String],
      timeout: Duration,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Result] = {

    val baseService: command.Svc = command
      .createService(channel)
      .withInterceptors(TraceContextGrpc.clientInterceptor)

    val service = token.fold(baseService)(LedgerCallCredentials.authenticatingStub(baseService, _))

    for {
      request <- EitherT.fromEither[Future](command.createRequest())
      response <- submitRequest(command)(instanceName, service, request, timeout)
      result <- EitherT.fromEither[Future](command.handleResponse(response))
    } yield result
  }

  private def submitRequest[Svc <: AbstractStub[Svc], Req, Res, Result](
      command: GrpcAdminCommand[Req, Res, Result]
  )(instanceName: String, service: command.Svc, request: Req, timeout: Duration)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Res] =
    CantonGrpcUtil
      .sendGrpcRequest(service, instanceName)(
        command.submitRequest(_, request),
        LoggerUtil.truncateString(maxRequestDebugLines, maxRequestDebugStringLength)(
          command.toString
        ),
        timeout,
        logger,
        CantonGrpcUtil.silentLogPolicy, // silent log policy, as the ConsoleEnvironment will log the result
        _ => false, // no retry to optimize for low latency
      )
      .leftMap(_.toString)
}
