// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.grpc.GrpcMetricsServerInterceptor
import com.digitalasset.canton.config.KeepAliveServerConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.error.ErrorInterceptor
import com.google.protobuf.Message
import io.grpc.*
import io.grpc.netty.NettyServerBuilder
import io.netty.handler.ssl.SslContext

import java.io.IOException
import java.net.{BindException, InetAddress, InetSocketAddress}
import java.util.concurrent.{Executor, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.control.NoStackTrace

object GrpcServer {

  // Unfortunately, we can't get the maximum inbound message size from the client, so we don't know
  // how big this should be. This seems long enough to contain useful data, but short enough that it
  // won't break most well-configured clients.
  // As the default response header limit for a Netty client is 8 KB, we set our limit to 4 KB to
  // allow for extra information such as the exception stack trace.
  private val MaximumStatusDescriptionLength = 4 * 1024 // 4 KB

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.IsInstanceOf"))
  def owner(
      address: Option[String],
      desiredPort: Port,
      maxInboundMessageSize: Int,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: LedgerApiServerMetrics,
      servicesExecutor: Executor,
      services: Iterable[BindableService],
      loggerFactory: NamedLoggerFactory,
      keepAlive: Option[KeepAliveServerConfig],
  ): ResourceOwner[Server] = {
    val host = address.map(InetAddress.getByName).getOrElse(InetAddress.getLoopbackAddress)
    val builder =
      NettyServerBuilder
        .forAddress(new InetSocketAddress(host, desiredPort.unwrap))
        .sslContext(sslContext.orNull)
        .executor(servicesExecutor)
        .maxInboundMessageSize(maxInboundMessageSize)
        .addTransportFilter(GrpcConnectionLogger(loggerFactory))

    val builderWithKeepAlive = configureKeepAlive(keepAlive, builder)
    // NOTE: Interceptors run in the reverse order in which they were added.
    val builderWithInterceptors =
      interceptors
        .foldLeft(builderWithKeepAlive) { case (builder, interceptor) =>
          builder.intercept(interceptor)
        }
        .intercept(new ActiveStreamMetricsInterceptor(metrics))
        .intercept(new GrpcMetricsServerInterceptor(metrics.grpc))
        .intercept(new TruncatedStatusInterceptor(MaximumStatusDescriptionLength))
        .intercept(new ErrorInterceptor(loggerFactory))

    val builderWithServices = services.foldLeft(builderWithInterceptors) {
      case (builder, service) =>
        toLegacyService(service).fold(builder.addService(service))(legacyService =>
          builder
            .addService(service)
            .addService(legacyService)
        )
    }
    ResourceOwner
      .forServer(builderWithServices, shutdownTimeout = 1.second)
      .transform(_.recoverWith {
        case e: IOException if e.getCause != null && e.getCause.isInstanceOf[BindException] =>
          Failure(new UnableToBind(desiredPort, e.getCause))
      })
  }

  def configureKeepAlive(
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

  final class UnableToBind(port: Port, cause: Throwable)
      extends RuntimeException(
        s"The API server was unable to bind to port $port. Terminate the process occupying the port, or choose a different one.",
        cause,
      )
      with NoStackTrace

  // This exposes the existing services under com.daml also under com.digitalasset.
  // This is necessary to allow applications built with an earlier version of the SDK
  // to still work.
  // The "proxy" services will not show up on the reflection service, because of the way it
  // processes service definitions via protobuf file descriptors.
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def toLegacyService(service: BindableService): Option[ServerServiceDefinition] = {
    val `com.daml` = "com.daml"
    val `com.digitalasset` = "com.digitalasset"

    val damlDef = service.bindService()
    val damlDesc = damlDef.getServiceDescriptor
    // Only add "proxy" services if it actually contains com.daml in the service name.
    // There are other services registered like the reflection service, that doesn't need the special treatment.
    if (damlDesc.getName.contains(`com.daml`)) {
      val digitalassetName = damlDesc.getName.replace(`com.daml`, `com.digitalasset`)
      val digitalassetDef = ServerServiceDefinition.builder(digitalassetName)
      damlDef.getMethods.forEach { methodDef =>
        val damlMethodDesc = methodDef.getMethodDescriptor
        val digitalassetMethodName =
          damlMethodDesc.getFullMethodName.replace(`com.daml`, `com.digitalasset`)
        val digitalassetMethodDesc =
          damlMethodDesc.toBuilder.setFullMethodName(digitalassetMethodName).build()
        val _ = digitalassetDef.addMethod(
          digitalassetMethodDesc.asInstanceOf[MethodDescriptor[Message, Message]],
          methodDef.getServerCallHandler.asInstanceOf[ServerCallHandler[Message, Message]],
        )
      }
      Option(digitalassetDef.build())
    } else None
  }

}
