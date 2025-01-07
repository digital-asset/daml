// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{KeepAliveServerConfig, TlsServerConfig}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerInterceptor

import java.util.concurrent.Executor
import scala.util.{Failure, Success}

object LedgerApiService {

  def apply(
      apiServices: ApiServices,
      desiredPort: Port,
      maxInboundMessageSize: Int,
      address: Option[String],
      tlsConfiguration: Option[TlsServerConfig],
      interceptors: List[ServerInterceptor] = List.empty,
      servicesExecutor: Executor,
      metrics: LedgerApiServerMetrics,
      keepAlive: Option[KeepAliveServerConfig],
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[ApiService] = {
    val logger = loggerFactory.getTracedLogger(this.getClass)
    implicit val traceContext = TraceContext.empty
    val _ = tlsConfiguration.map(_.setJvmTlsProperties())
    val sslContext = tlsConfiguration.map(
      CantonServerBuilder.sslContext(_, logTlsProtocolAndCipherSuites = true)
    )
    ResourceOwner
      .forCloseable(() => apiServices)
      .flatMap(_ =>
        GrpcServer
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
            keepAlive = keepAlive,
          )
      )
      .map { server =>
        val host = address.getOrElse("localhost")
        val actualPort = server.getPort
        val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
        val withKeepAlive = keepAlive.fold("")(ka => s" with $ka")
        logger.info(s"Listening on $host:$actualPort over $transportMedium$withKeepAlive.")
        new ApiService {
          override val port: Port =
            Port.tryCreate(server.getPort)
        }
      }
      .transformWith {
        case Failure(ex) =>
          logger.error("Failed to create LedgerApiServer", ex)
          ResourceOwner.failed(ex)
        case Success(s) => ResourceOwner.successful(s)
      }
  }

}
