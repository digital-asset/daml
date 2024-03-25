// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.metrics.grpc.{GrpcMetricsServerInterceptor, GrpcServerMetrics}
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{TraceContextGrpc, TracingConfig}
import io.grpc.ServerInterceptors.intercept
import io.grpc.ServerServiceDefinition

import scala.util.chaining.*

trait CantonServerInterceptors {
  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition
}

class CantonCommunityServerInterceptors(
    tracingConfig: TracingConfig,
    apiLoggingConfig: ApiLoggingConfig,
    loggerFactory: NamedLoggerFactory,
    grpcMetrics: GrpcServerMetrics,
) extends CantonServerInterceptors {
  private def interceptForLogging(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    if (withLogging) {
      intercept(service, new ApiRequestLogger(loggerFactory, apiLoggingConfig))
    } else {
      service
    }

  private def addTraceContextInterceptor(
      service: ServerServiceDefinition
  ): ServerServiceDefinition =
    tracingConfig.propagation match {
      case TracingConfig.Propagation.Disabled => service
      case TracingConfig.Propagation.Enabled =>
        intercept(service, TraceContextGrpc.serverInterceptor)
    }

  private def addMetricsInterceptor(
      service: ServerServiceDefinition
  ): ServerServiceDefinition =
    intercept(service, new GrpcMetricsServerInterceptor(grpcMetrics))

  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    service
      .pipe(interceptForLogging(_, withLogging))
      .pipe(addTraceContextInterceptor)
      .pipe(addMetricsInterceptor)
}
