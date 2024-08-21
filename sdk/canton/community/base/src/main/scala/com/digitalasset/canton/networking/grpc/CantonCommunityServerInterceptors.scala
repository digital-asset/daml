// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.metrics.grpc.{GrpcMetricsServerInterceptor, GrpcServerMetrics}
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.auth.{
  AdminAuthorizer,
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
    authServices: Seq[AuthServiceConfig],
    adminToken: Option[CantonAdminToken],
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
    val authService = new CantonAdminTokenAuthService(
      adminToken,
      parent = authServices.map(
        _.create(
          // TODO(i20232): configure jwt leeway for admin api's
          None,
          loggerFactory,
        )
      ),
    )
    val interceptor = new AuthorizationInterceptor(
      authService,
      // TODO(i20232): add telemetry
      NoOpTelemetry,
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
