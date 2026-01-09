// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.config.KeepAliveServerConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext
import io.grpc.{BindableService, ForwardingServerBuilder, Server, ServerInterceptor}

import java.io.IOException
import java.net.BindException
import java.util.concurrent.Executor
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.control.NoStackTrace

object GrpcServerOwner {

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  def apply(
      address: Option[String],
      desiredPort: Port,
      maxInboundMessageSize: Int,
      maxInboundMetadataSize: Int,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: LedgerApiServerMetrics,
      servicesExecutor: Executor,
      services: Iterable[BindableService],
      loggerFactory: NamedLoggerFactory,
      keepAlive: Option[KeepAliveServerConfig],
  ): ResourceOwner[Server] = {

    val allInterceptors = GrpcInterceptors(metrics, loggerFactory, interceptors)

    def build[T <: ForwardingServerBuilder[T]](builderWithServices: T) =
      ResourceOwner
        .forServer(builderWithServices, shutdownTimeout = 1.second)
        .transform(_.recoverWith {
          case e: IOException if e.getCause != null && e.getCause.isInstanceOf[BindException] =>
            Failure(new UnableToBind(desiredPort, e.getCause))
        })

    for {
      _ <- build(GrpcServer.inProc(desiredPort, allInterceptors, services, servicesExecutor))
      httpServer <- build(
        GrpcServer.netty(
          address,
          desiredPort,
          maxInboundMessageSize,
          maxInboundMetadataSize,
          sslContext,
          keepAlive,
          allInterceptors,
          services,
          servicesExecutor,
          loggerFactory,
        )
      )
    } yield httpServer
  }

  final class UnableToBind(port: Port, cause: Throwable)
      extends RuntimeException(
        s"The API server was unable to bind to port $port. Terminate the process occupying the port, or choose a different one.",
        cause,
      )
      with NoStackTrace

}
