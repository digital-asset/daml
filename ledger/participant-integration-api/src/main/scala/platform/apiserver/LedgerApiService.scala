// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.util.concurrent.Executor
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.ports.Port
import io.grpc.ServerInterceptor

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

private[daml] final class LedgerApiService(
    apiServicesOwner: ResourceOwner[ApiServices],
    desiredPort: Port,
    maxInboundMessageSize: Int,
    address: Option[String],
    tlsConfiguration: Option[TlsConfiguration] = None,
    interceptors: List[ServerInterceptor] = List.empty,
    servicesExecutor: Executor,
    metrics: Metrics,
    rateLimitingConfig: Option[RateLimitingConfig],
)(implicit loggingContext: LoggingContext)
    extends ResourceOwner[ApiService] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit context: ResourceContext): Resource[ApiService] = {
    val servicesClosedPromise = Promise[Unit]()

    val apiServicesResource = apiServicesOwner.acquire()
    (for {
      apiServices <- apiServicesResource
      _ = tlsConfiguration.map(_.setJvmTlsProperties())
      sslContext = tlsConfiguration.flatMap(_.server)
      server <- GrpcServer
        .owner(
          address,
          desiredPort,
          maxInboundMessageSize,
          sslContext,
          interceptors,
          metrics,
          servicesExecutor,
          apiServices.services,
          rateLimitingConfig,
        )
        .acquire()
      // Notify the caller that the services have been closed, so a reset request can complete
      // without blocking on the server terminating.
      _ <- Resource(Future.unit)(_ =>
        apiServicesResource.release().map(_ => servicesClosedPromise.success(()))
      )
    } yield {
      val host = address.getOrElse("localhost")
      val actualPort = server.getPort
      val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
      logger.info(s"Listening on $host:$actualPort over $transportMedium.")
      new ApiService {
        override val port: Port =
          Port(server.getPort)

        override def servicesClosed(): Future[Unit] =
          servicesClosedPromise.future
      }
    }).transformWith {
      case Failure(ex) =>
        logger.error("Failed to create LedgerApiServer", ex)
        Resource.failed(ex)
      case Success(s) => Resource.successful(s)
    }
  }
}
