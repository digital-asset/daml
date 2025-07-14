// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.jwt.JwtTimestampLeeway
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.{
  AdminAuthorizer,
  AuthInterceptor,
  AuthServiceWildcard,
  CantonAdminToken,
  CantonAdminTokenAuthService,
  GrpcAuthInterceptor,
}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.AuthServiceConfig
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.ServerServiceDefinition

object CantonCommunityAuthInterceptorDefinition {
  def addAuthInterceptor(
      service: ServerServiceDefinition,
      loggerFactory: NamedLoggerFactory,
      authServiceConfigs: Seq[AuthServiceConfig],
      adminToken: Option[CantonAdminToken],
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      telemetry: Telemetry,
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
    val genericInterceptor = new AuthInterceptor(
      authServices,
      loggerFactory,
      DirectExecutionContext(
        loggerFactory.getLogger(AuthInterceptor.getClass)
      ),
      AdminAuthorizer,
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
