// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.IOException
import java.net.{BindException, InetAddress, InetSocketAddress}
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit.SECONDS

import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.metrics.Metrics
import com.daml.ports.Port
import com.daml.resources.grpc.ServerResourceOwner
import com.google.protobuf.Message
import io.grpc._
import io.grpc.netty.NettyServerBuilder
import io.netty.handler.ssl.SslContext

import scala.concurrent.duration.DurationInt
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

private[apiserver] object GrpcServer {

  // Unfortunately, we can't get the maximum inbound message size from the client, so we don't know
  // how big this should be. This seems long enough to contain useful data, but short enough that it
  // won't break most well-configured clients.
  // As the default response header limit for a Netty client is 8 KB, we set our limit to 4 KB to
  // allow for extra information such as the exception stack trace.
  private val MaximumStatusDescriptionLength = 4 * 1024 // 4 KB

  private def makeBuilder(
      address: Option[String],
      desiredPort: Port,
      maxInboundMessageSize: Int,
      sslContext: Option[SslContext],
      interceptors: List[ServerInterceptor],
      metrics: Metrics,
      servicesExecutor: Executor,
      services: Iterable[BindableService],
  ): NettyServerBuilder = {
    val host = address.map(InetAddress.getByName).getOrElse(InetAddress.getLoopbackAddress)
    val builder = NettyServerBuilder.forAddress(new InetSocketAddress(host, desiredPort.value))
    builder.sslContext(sslContext.orNull)
    builder.permitKeepAliveTime(10, SECONDS)
    builder.permitKeepAliveWithoutCalls(true)
    builder.executor(servicesExecutor)
    builder.maxInboundMessageSize(maxInboundMessageSize)
    interceptors.foreach(builder.intercept)
    builder.intercept(new MetricsInterceptor(metrics))
    builder.intercept(new TruncatedStatusInterceptor(MaximumStatusDescriptionLength))
    services.foreach { service =>
      builder.addService(service)
      toLegacyService(service).foreach(builder.addService)
    }
    builder
  }

  private def causedByBindException(e: IOException): Boolean =
    e.getCause != null && e.getCause.isInstanceOf[BindException]

  final class Owner(
      address: Option[String],
      desiredPort: Port,
      maxInboundMessageSize: Int,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: Metrics,
      servicesExecutor: Executor,
      services: Iterable[BindableService],
  ) extends ServerResourceOwner[ResourceContext](
        builder = makeBuilder(
          address,
          desiredPort,
          maxInboundMessageSize,
          sslContext,
          interceptors,
          metrics,
          servicesExecutor,
          services,
        ),
        shutdownTimeout = 1.second,
      ) {
    override def acquire()(implicit context: ResourceContext): Resource[Server] = {
      super.acquire().transformWith {
        case Success(server) =>
          Resource.successful(server)
        case Failure(e: IOException) if causedByBindException(e) =>
          Resource.failed(new UnableToBind(desiredPort, e.getCause))
        case Failure(exception) =>
          Resource.failed(exception)
      }
    }
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
