// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.tls.TlsConfiguration
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerInterceptor

import java.util.concurrent.Executor
import scala.util.{Failure, Success}

final class LedgerApiService(
    apiServicesOwner: ResourceOwner[ApiServices],
    desiredPort: Port,
    maxInboundMessageSize: Int,
    address: Option[String],
    tlsConfiguration: Option[TlsConfiguration] = None,
    interceptors: List[ServerInterceptor] = List.empty,
    servicesExecutor: Executor,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
) extends ResourceOwner[ApiService]
    with NamedLogging {

  override def acquire()(implicit context: ResourceContext): Resource[ApiService] = {
    implicit val traceContext = TraceContext.empty

    (for {
      apiServices <- apiServicesOwner.acquire()
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
          loggerFactory,
        )
        .acquire()
    } yield {
      val host = address.getOrElse("localhost")
      val actualPort = server.getPort
      val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
      logger.info(s"Listening on $host:$actualPort over $transportMedium.")
      new ApiService {
        override val port: Port =
          Port.tryCreate(server.getPort)
      }
    }).transformWith {
      case Failure(ex) =>
        logger.error("Failed to create LedgerApiServer", ex)
        Resource.failed(ex)
      case Success(s) => Resource.successful(s)
    }
  }
}
