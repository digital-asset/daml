// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.jwt.JwtTimestampLeeway
import com.daml.metrics.grpc.{GrpcMetricsServerInterceptor, GrpcServerMetrics}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.{
  AdminAuthorizer,
  AuthServiceWildcard,
  AuthorizationInterceptor,
  CantonAdminToken,
  CantonAdminTokenAuthService,
}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.{ApiLoggingConfig, AuthServiceConfig}
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
    authServiceConfigs: Seq[AuthServiceConfig],
    adminToken: Option[CantonAdminToken],
    jwtTimestampLeeway: Option[JwtTimestampLeeway],
    telemetry: Telemetry,
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

  private def addAuthorizationInterceptor(
      service: ServerServiceDefinition
  ): ServerServiceDefinition = {
    val authServices = new CantonAdminTokenAuthService(adminToken) +:
      (if (authServiceConfigs.isEmpty)
         List(AuthServiceWildcard)
       else
         authServiceConfigs.map(
           _.create(
             jwtTimestampLeeway,
             loggerFactory,
           )
         ))
    val interceptor = new AuthorizationInterceptor(
      authServices,
      telemetry,
      loggerFactory,
      DirectExecutionContext(loggerFactory.getLogger(AuthorizationInterceptor.getClass)),
      AdminAuthorizer,
    )
    intercept(service, interceptor)
  }

  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    service
      .pipe(interceptForLogging(_, withLogging))
      .pipe(addTraceContextInterceptor)
      .pipe(addMetricsInterceptor)
      .pipe(addAuthorizationInterceptor)
}
