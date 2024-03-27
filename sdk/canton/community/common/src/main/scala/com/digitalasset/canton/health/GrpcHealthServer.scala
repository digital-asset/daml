// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.config.*
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.lifecycle.Lifecycle.toCloseableServer
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.tracing.TracingConfig
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.protobuf.services.ProtoReflectionService

import java.util.concurrent.ExecutorService

class GrpcHealthServer(
    config: GrpcHealthServerConfig,
    metrics: LabeledMetricsFactory,
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
      MetricName.Daml :+ "health",
      metrics,
      executor,
      loggerFactory,
      apiConfig,
      tracingConfig,
      grpcMetrics,
    )
    .addService(ProtoReflectionService.newInstance(), withLogging = false)
    .addService(healthManager.getHealthService.bindService())
    .build
    .start()

  private val closeable = toCloseableServer(server, logger, "HealthServer")

  private def setStatus(serviceName: String, status: ServingStatus): Unit = {
    healthManager.setStatus(
      serviceName,
      status,
    )
  }

  override def onClosed(): Unit = {
    healthManager.enterTerminalState()
    executor.shutdown()
    closeable.close()
  }
}
