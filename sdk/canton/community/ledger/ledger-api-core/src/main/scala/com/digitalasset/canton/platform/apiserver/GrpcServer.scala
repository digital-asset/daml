// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.canton.config.KeepAliveServerConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.*
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{Executor, TimeUnit}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object GrpcServer {
  def netty(
      address: Option[String],
      desiredPort: Port,
      maxInboundMessageSize: Int,
      maxInboundMetadataSize: Int,
      sslContext: Option[SslContext] = None,
      keepAlive: Option[KeepAliveServerConfig],
      interceptors: List[ServerInterceptor] = List.empty,
      services: Iterable[BindableService],
      servicesExecutor: Executor,
      loggerFactory: NamedLoggerFactory,
  ): NettyServerBuilder = {
    val host = address.map(InetAddress.getByName).getOrElse(InetAddress.getLoopbackAddress)
    val builder =
      NettyServerBuilder
        .forAddress(new InetSocketAddress(host, desiredPort.unwrap))
        .sslContext(sslContext.orNull)
        .executor(servicesExecutor)
        .maxInboundMessageSize(maxInboundMessageSize)
        .maxInboundMetadataSize(maxInboundMetadataSize)
        .addTransportFilter(GrpcConnectionLogger(loggerFactory))

    val builderWithKeepAlive = configureKeepAlive(keepAlive, builder)
    addServicesAndInterceptors(builderWithKeepAlive, interceptors, services)
  }
  def inProc(
      desiredPort: Port,
      interceptors: List[ServerInterceptor] = List.empty,
      services: Iterable[BindableService],
      servicesExecutor: Executor,
  ): InProcessServerBuilder = {
    val inProcessBuilder: InProcessServerBuilder =
      InProcessServerBuilder
        .forName(InProcessGrpcName.forPort(desiredPort))
        .executor(servicesExecutor)
    addServicesAndInterceptors(inProcessBuilder, interceptors, services)
  }

  private def addServicesAndInterceptors[T <: ForwardingServerBuilder[T]](
      builder: T,
      interceptors: List[ServerInterceptor],
      services: Iterable[BindableService],
  ) = {
    // NOTE: Interceptors run in the reverse order in which they were added.
    val builderWithInterceptors = interceptors.foldLeft(builder) { case (builder, interceptor) =>
      builder.intercept(interceptor)
    }
    services.foldLeft(builderWithInterceptors) { case (builder, service) =>
      builder.addService(service)
    }
  }

  private def configureKeepAlive(
      keepAlive: Option[KeepAliveServerConfig],
      builder: NettyServerBuilder,
  ): NettyServerBuilder =
    keepAlive.fold(builder) { ka =>
      val time = ka.time.unwrap.toMillis
      val timeout = ka.timeout.unwrap.toMillis
      val permitTime = ka.permitKeepAliveTime.unwrap.toMillis
      val permitKAWOCalls = ka.permitKeepAliveWithoutCalls
      builder
        .keepAliveTime(time, TimeUnit.MILLISECONDS)
        .keepAliveTimeout(timeout, TimeUnit.MILLISECONDS)
        .permitKeepAliveTime(
          permitTime,
          TimeUnit.MILLISECONDS,
        ) // gracefully allowing a bit more aggressive keep alives from clients
        .permitKeepAliveWithoutCalls(permitKAWOCalls)
    }
}
