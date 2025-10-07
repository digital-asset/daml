// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.jwt.JwtTimestampLeeway
import com.daml.metrics.grpc.{GrpcMetricsServerInterceptor, GrpcServerMetrics}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.config.{
  AdminTokenConfig,
  ApiLoggingConfig,
  AuthServiceConfig,
  JwksCacheConfig,
  StreamLimitConfig,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ratelimiting.{
  RateLimitingInterceptor,
  StreamCounterCheck,
}
import com.digitalasset.canton.tracing.{TraceContextGrpc, TracingConfig}
import io.grpc.ServerInterceptors.intercept
import io.grpc.{ServerInterceptor, ServerServiceDefinition}

import scala.util.chaining.*

trait CantonServerInterceptors {
  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition

  def streamCounterCheck: Option[StreamCounterCheck]
}

class CantonCommunityServerInterceptors(
    tracingConfig: TracingConfig,
    apiLoggingConfig: ApiLoggingConfig,
    loggerFactory: NamedLoggerFactory,
    grpcMetrics: GrpcServerMetrics,
    authServiceConfigs: Seq[AuthServiceConfig],
    adminTokenDispenser: Option[CantonAdminTokenDispenser],
    jwtTimestampLeeway: Option[JwtTimestampLeeway],
    adminTokenConfig: AdminTokenConfig,
    jwksCacheConfig: JwksCacheConfig,
    telemetry: Telemetry,
    additionalInterceptors: Seq[ServerInterceptor] = Seq.empty,
    streamLimits: Option[StreamLimitConfig],
) extends CantonServerInterceptors {

  override val streamCounterCheck: Option[StreamCounterCheck] = streamLimits.map { limits =>
    new StreamCounterCheck(limits.limits, limits.warnOnUndefinedLimits, loggerFactory)
  }

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

  private def addAuthInterceptor(
      service: ServerServiceDefinition
  ): ServerServiceDefinition =
    CantonCommunityAuthInterceptorDefinition
      .addAuthInterceptor(
        service,
        loggerFactory,
        authServiceConfigs,
        adminTokenDispenser,
        jwtTimestampLeeway,
        adminTokenConfig,
        jwksCacheConfig,
        telemetry,
      )

  private def addLimitInterceptor(service: ServerServiceDefinition): ServerServiceDefinition =
    streamCounterCheck.fold(service) { limits =>
      intercept(service, new RateLimitingInterceptor(List(limits.check)), limits)
    }

  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    service
      .pipe(interceptForLogging(_, withLogging))
      .pipe(addTraceContextInterceptor)
      .pipe(addMetricsInterceptor)
      .pipe(addLimitInterceptor)
      .pipe(addAuthInterceptor)
      .pipe(s => additionalInterceptors.foldLeft(s)((acc, i) => intercept(acc, i)))
}
