// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.ports.Port
import com.daml.resources.{Resource, ResourceOwner}
import io.grpc.ServerInterceptor
import io.netty.handler.ssl.SslContext

import scala.concurrent.{ExecutionContext, Future, Promise}

final class LedgerApiServer(
    apiServicesOwner: ResourceOwner[ApiServices],
    desiredPort: Port,
    maxInboundMessageSize: Int,
    address: Option[String],
    sslContext: Option[SslContext] = None,
    interceptors: List[ServerInterceptor] = List.empty,
    metrics: Metrics,
)(implicit actorSystem: ActorSystem, materializer: Materializer, logCtx: LoggingContext)
    extends ResourceOwner[ApiServer] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit executionContext: ExecutionContext): Resource[ApiServer] = {
    val servicesClosedPromise = Promise[Unit]()

    for {
      eventLoopGroups <- new ServerEventLoopGroups.Owner(
        actorSystem.name,
        workerParallelism = sys.runtime.availableProcessors(),
        bossParallelism = 1,
      ).acquire()
      apiServicesResource = apiServicesOwner.acquire()
      apiServices <- apiServicesResource
      server <- new GrpcServerOwner(
        address,
        desiredPort,
        maxInboundMessageSize,
        sslContext,
        interceptors,
        metrics,
        eventLoopGroups,
        apiServices.services,
      ).acquire()
      // Notify the caller that the services have been closed, so a reset request can complete
      // without blocking on the server terminating.
      _ <- Resource(Future.successful(()))(_ =>
        apiServicesResource.release().map(_ => servicesClosedPromise.success(())))
    } yield {
      val host = address.getOrElse("localhost")
      val actualPort = server.getPort
      val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
      logger.info(s"Listening on $host:$actualPort over $transportMedium.")
      new ApiServer {
        override val port: Port =
          Port(server.getPort)

        override def servicesClosed(): Future[Unit] =
          servicesClosedPromise.future
      }
    }
  }
}
