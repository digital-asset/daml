// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import com.daml.grpc.reflection.{ServerReflectionClient, ServiceDescriptorInfo}
import com.daml.resources.grpc.GrpcResourceOwnerFactories
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ResourceOwnerFactories}
import io.grpc.{Channel, Server, ServerBuilder, ServerInterceptor, ServerInterceptors}
import io.grpc.reflection.v1alpha.ServerReflectionGrpc

import scala.concurrent.duration.DurationInt

object ReverseProxy {

  private def proxyServices(
      backend: Channel,
      serverBuilder: ServerBuilder[_],
      services: Set[ServiceDescriptorInfo],
      interceptors: Map[String, Seq[ServerInterceptor]],
  ): Unit =
    for (service <- services) {
      serverBuilder.addService(
        ServerInterceptors.interceptForward(
          ForwardService(backend, service),
          interceptors.getOrElse(service.fullServiceName, Seq.empty): _*
        )
      )
    }

  def owner[Context](
      backend: Channel,
      serverBuilder: ServerBuilder[_],
      interceptors: Map[String, Seq[ServerInterceptor]],
  )(implicit context: HasExecutionContext[Context]): AbstractResourceOwner[Context, Server] = {
    val factory = new ResourceOwnerFactories[Context] with GrpcResourceOwnerFactories[Context] {
      override protected implicit val hasExecutionContext: HasExecutionContext[Context] =
        context
    }
    val stub = ServerReflectionGrpc.newStub(backend)
    val client = new ServerReflectionClient(stub)
    for {
      services <- factory.forFuture(() => client.getAllServices())
      _ = proxyServices(backend, serverBuilder, services, interceptors)
      server <- factory.forServer(serverBuilder, shutdownTimeout = 5.seconds)
    } yield server
  }

}
