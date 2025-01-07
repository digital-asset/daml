// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.daml.metrics.grpc.GrpcServerMetrics
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.config.*
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.lifecycle.LifeCycle.toCloseableServer
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.tracing.TracingConfig
import io.grpc.protobuf.services.ProtoReflectionService

import java.util.concurrent.ExecutorService

class GrpcHealthServer(
    config: GrpcHealthServerConfig,
    executor: ExecutorService,
    override val loggerFactory: NamedLoggerFactory,
    apiConfig: ApiLoggingConfig,
    tracingConfig: TracingConfig,
    grpcMetrics: GrpcServerMetrics,
    override val timeouts: ProcessingTimeout,
    healthManager: io.grpc.protobuf.services.HealthStatusManager,
) extends NamedLogging
    with FlagCloseable {
  private val server = CantonServerBuilder
    .forConfig(
      config,
      None,
      executor,
      loggerFactory,
      apiConfig,
      tracingConfig,
      grpcMetrics,
      NoOpTelemetry,
    )
    .addService(ProtoReflectionService.newInstance(), withLogging = false)
    .addService(healthManager.getHealthService.bindService())
    .build
    .start()

  private val closeable = toCloseableServer(server, logger, "HealthServer")

  override def onClosed(): Unit = {
    healthManager.enterTerminalState()
    executor.shutdown()
    closeable.close()
  }
}
