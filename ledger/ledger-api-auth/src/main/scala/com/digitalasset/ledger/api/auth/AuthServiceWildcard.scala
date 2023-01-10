// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import io.grpc.Metadata

/** An AuthService that authorizes all calls by always returning a wildcard [[ClaimSet.Claims]] */
object AuthServiceWildcard extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] = {
    CompletableFuture.completedFuture(ClaimSet.Claims.Wildcard)
  }
}
