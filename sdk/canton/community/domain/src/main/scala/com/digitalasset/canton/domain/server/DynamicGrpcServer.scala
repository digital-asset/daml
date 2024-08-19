// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.server

import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.config.PublicServerConfig
import com.digitalasset.canton.domain.sequencing.SequencerRuntime
import com.digitalasset.canton.environment.HasGeneralCantonNodeParameters
import com.digitalasset.canton.health.{
  DependenciesHealthService,
  GrpcHealthReporter,
  ServiceHealthStatusManager,
}
import com.digitalasset.canton.lifecycle.Lifecycle.{CloseableServer, toCloseableServer}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import io.grpc.protobuf.services.ProtoReflectionService

import scala.concurrent.ExecutionContextExecutorService

/** Creates a dynamic public server to which services can be added at a later time,
  * while providing a gRPC health service from the start.
  * This is useful to bring up the sequencer node endpoint with health service, prior to being
  * initialized.
  */
class DynamicGrpcServer(
    val loggerFactory: NamedLoggerFactory,
    maxRequestSize: MaxRequestSize,
    nodeParameters: HasGeneralCantonNodeParameters,
    serverConfig: PublicServerConfig,
    grpcMetrics: GrpcServerMetrics,
    grpcHealthReporter: GrpcHealthReporter,
    healthService: DependenciesHealthService,
)(implicit executionContext: ExecutionContextExecutorService)
    extends NamedLogging {
  private lazy val grpcHealthManager =
    ServiceHealthStatusManager(
      "Health API",
      new io.grpc.protobuf.services.HealthStatusManager(),
      Set(healthService),
    )

  grpcHealthReporter.registerHealthManager(grpcHealthManager)

  // Upon initialization, register all gRPC services into their dynamic slot
  def initialize(runtime: SequencerRuntime): DynamicGrpcServer = {
    runtime.sequencerServices.foreach(registry.addServiceU(_))
    this
  }

  private val (grpcServer, registry) = {
    val serverBuilder = CantonServerBuilder
      .forConfig(
        serverConfig,
        executionContext,
        loggerFactory,
        nodeParameters.loggingConfig.api,
        nodeParameters.tracing,
        grpcMetrics,
      )
      // Overriding the dummy setting from PublicServerConfig.
      // To avoid being locked out if the dynamic domain parameter maxRequestSize is too small.
      .maxInboundMessageSize(
        serverConfig.overrideMaxRequestSize.getOrElse(maxRequestSize.value)
      )

    val registry = serverBuilder.mutableHandlerRegistry()

    serverBuilder
      .addService(grpcHealthManager.manager.getHealthService.bindService())
      .addService(ProtoReflectionService.newInstance(), withLogging = false)
      .discard[CantonServerBuilder]

    (toCloseableServer(serverBuilder.build.start(), logger, "PublicServer"), registry)
  }

  val publicServer: CloseableServer = grpcServer
}
