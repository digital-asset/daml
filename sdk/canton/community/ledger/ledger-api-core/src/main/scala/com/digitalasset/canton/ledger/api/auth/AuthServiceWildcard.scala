// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata

import java.util.concurrent.{CompletableFuture, CompletionStage}

/** An AuthService that authorizes all calls by always returning a wildcard [[ClaimSet.Claims]] */
object AuthServiceWildcard extends AuthService {
  override def decodeMetadata(
      headers: Metadata
  )(implicit traceContext: TraceContext): CompletionStage[ClaimSet] = {
    CompletableFuture.completedFuture(ClaimSet.Claims.Wildcard)
  }
}
