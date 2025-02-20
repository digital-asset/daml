// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import cats.data.EitherT
import com.daml.grpc.AuthCallCredentials
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcClient, GrpcManagedChannel}
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
    * @return
    *   Either a printable error as a String or a Unit indicating all was successful
    */
  def run[Req, Res, Result](
      instanceName: String,
      command: GrpcAdminCommand[Req, Res, Result],
      managedChannel: GrpcManagedChannel,
      token: Option[String],
      timeout: Duration,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Result] = {

    def service(channel: ManagedChannel): command.Svc = {
      val baseService = command
        .createServiceInternal(channel)
        .withInterceptors(TraceContextGrpc.clientInterceptor)
      token.toList.foldLeft(baseService)(AuthCallCredentials.authorizingStub)
    }
    val client = GrpcClient.create(managedChannel, service)

    for {
      request <- EitherT.fromEither[Future](command.createRequestInternal())
      response <- submitRequest(command)(instanceName, client, request, timeout)
      result <- EitherT.fromEither[Future](command.handleResponseInternal(response))
    } yield result
  }

  private def submitRequest[Svc <: AbstractStub[Svc], Req, Res, Result](
      command: GrpcAdminCommand[Req, Res, Result]
  )(instanceName: String, service: GrpcClient[command.Svc], request: Req, timeout: Duration)(
      implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Res] = CantonGrpcUtil.shutdownAsGrpcErrorE(
    CantonGrpcUtil
      .sendGrpcRequest(service, instanceName)(
        command.submitRequestInternal(_, request),
        LoggerUtil.truncateString(maxRequestDebugLines, maxRequestDebugStringLength)(
          command.toString
        ),
        timeout,
        logger,
        CantonGrpcUtil.SilentLogPolicy, // silent log policy, as the ConsoleEnvironment will log the result
        _ => false, // no retry to optimize for low latency
      )
      .leftMap(_.toString)
  )
}
