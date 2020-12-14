// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.IOException
import java.net.{BindException, InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.metrics.Metrics
import com.daml.ports.Port
import com.google.protobuf.Message
import io.grpc._
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContext

import scala.concurrent.Future
import scala.util.control.NoStackTrace

private[apiserver] object GrpcServer {

  // Unfortunately, we can't get the maximum inbound message size from the client, so we don't know
  // how big this should be. This seems long enough to contain useful data, but short enough that it
  // won't break most well-configured clients.
  // As the default response header limit for a Netty client is 8 KB, we set our limit to 4 KB to
  // allow for extra information such as the exception stack trace.
  private val MaximumStatusDescriptionLength = 4 * 1024 // 4 KB

  final class Owner(
      address: Option[String],
      desiredPort: Port,
      maxInboundMessageSize: Int,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: Metrics,
      eventLoopGroups: ServerEventLoopGroups,
      services: Iterable[BindableService],
  ) extends ResourceOwner[Server] {
    override def acquire()(implicit context: ResourceContext): Resource[Server] = {
      val host = address.map(InetAddress.getByName).getOrElse(InetAddress.getLoopbackAddress)
      Resource(Future {
        val builder = NettyServerBuilder.forAddress(new InetSocketAddress(host, desiredPort.value))
        builder.sslContext(sslContext.orNull)
        builder.channelType(classOf[NioServerSocketChannel])
        builder.permitKeepAliveTime(10, SECONDS)
        builder.permitKeepAliveWithoutCalls(true)
        builder.directExecutor()
        builder.maxInboundMessageSize(maxInboundMessageSize)
        interceptors.foreach(builder.intercept)
        builder.intercept(new MetricsInterceptor(metrics))
        builder.intercept(new TruncatedStatusInterceptor(MaximumStatusDescriptionLength))
        eventLoopGroups.populate(builder)
        services.foreach { service =>
          builder.addService(service)
          toLegacyService(service).foreach(builder.addService)
        }
        val server = builder.build()
        try {
          server.start()
        } catch {
          case e: IOException if e.getCause != null && e.getCause.isInstanceOf[BindException] =>
            throw new UnableToBind(desiredPort, e.getCause)
        }
        server
      })(server =>
        Future {
          // Phase 1, initialize shutdown, but wait for termination.
          // If the shutdown has been initiated by the reset service, this gives the service time to gracefully complete the request.
          server.shutdown()
          server.awaitTermination(1, TimeUnit.SECONDS)

          // Phase 2: Now cut off all remaining connections.
          server.shutdownNow()
          server.awaitTermination()
      })
    }
  }

  final class UnableToBind(port: Port, cause: Throwable)
      extends RuntimeException(
        s"The API server was unable to bind to port $port. Terminate the process occupying the port, or choose a different one.",
        cause)
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
          methodDef.getServerCallHandler.asInstanceOf[ServerCallHandler[Message, Message]]
        )
      }
      Option(digitalassetDef.build())
    } else None
  }

}
