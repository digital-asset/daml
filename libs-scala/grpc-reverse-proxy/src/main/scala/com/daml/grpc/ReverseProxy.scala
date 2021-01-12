// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import com.daml.grpc.reflection.ServerReflectionClient
import io.grpc._
import io.grpc.reflection.v1alpha.ServerReflectionGrpc

import scala.concurrent.{ExecutionContext, Future}

object ReverseProxy {

  def create(
      backend: Channel,
      serverBuilder: ServerBuilder[_],
  )(implicit
      ec: ExecutionContext
  ): Future[Server] = create(backend, serverBuilder, Map.empty[String, Seq[ServerInterceptor]])

  def create(
      backend: Channel,
      serverBuilder: ServerBuilder[_],
      interceptor: (String, Seq[ServerInterceptor]),
      interceptors: (String, Seq[ServerInterceptor])*
  )(implicit
      ec: ExecutionContext
  ): Future[Server] = create(backend, serverBuilder, Map(interceptor +: interceptors: _*))

  private def create(
      backend: Channel,
      serverBuilder: ServerBuilder[_],
      interceptors: Map[String, Seq[ServerInterceptor]],
  )(implicit
      ec: ExecutionContext
  ): Future[Server] = {
    val stub = ServerReflectionGrpc.newStub(backend)
    val client = new ServerReflectionClient(stub)
    val future = client.getAllServices()
    future
      .map { services =>
        for (service <- services) {
          serverBuilder.addService(
            ServerInterceptors.interceptForward(
              ForwardService(backend, service),
              interceptors.getOrElse(service.fullServiceName, Seq.empty): _*
            )
          )
        }
        serverBuilder.build()
      }
  }

}
