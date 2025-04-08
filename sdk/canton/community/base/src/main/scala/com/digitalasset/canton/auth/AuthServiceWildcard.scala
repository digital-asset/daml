// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata

import scala.concurrent.Future

/** An AuthService that authorizes all calls by always returning a wildcard [[ClaimSet.Claims]] */
object AuthServiceWildcard extends AuthService {
  override def decodeMetadata(
      headers: Metadata,
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[ClaimSet] =
    Future.successful(ClaimSet.Claims.Wildcard)
}
