// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.jwt.JwtTimestampLeeway
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.{
  AuthInterceptor,
  AuthServiceWildcard,
  CantonAdminTokenAuthService,
  CantonAdminTokenDispenser,
  GrpcAuthInterceptor,
  RequiringAdminClaimResolver,
}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.{AdminTokenConfig, AuthServiceConfig, JwksCacheConfig}
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.ServerServiceDefinition

object CantonCommunityAuthInterceptorDefinition {
  def addAuthInterceptor(
      service: ServerServiceDefinition,
      loggerFactory: NamedLoggerFactory,
      authServiceConfigs: Seq[AuthServiceConfig],
      adminTokenDispenser: Option[CantonAdminTokenDispenser],
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      adminTokenConfig: AdminTokenConfig,
      jwksCacheConfig: JwksCacheConfig,
      telemetry: Telemetry,
  ): ServerServiceDefinition = {
    val authServices =
      if (authServiceConfigs.isEmpty)
        List(AuthServiceWildcard)
      else {
        adminTokenDispenser
          .map(new CantonAdminTokenAuthService(_, None, adminTokenConfig))
          .toList ++
          authServiceConfigs.map(
            _.create(
              jwksCacheConfig,
              jwtTimestampLeeway,
              loggerFactory,
            )
          )
      }
    val genericInterceptor = new AuthInterceptor(
      authServices,
      loggerFactory,
      DirectExecutionContext(
        loggerFactory.getLogger(AuthInterceptor.getClass)
      ),
      RequiringAdminClaimResolver,
    )
    val interceptor = new GrpcAuthInterceptor(
      genericInterceptor,
      telemetry,
      loggerFactory,
      DirectExecutionContext(
        loggerFactory.getLogger(AuthInterceptor.getClass)
      ),
    )
    io.grpc.ServerInterceptors.intercept(service, interceptor)
  }

}
