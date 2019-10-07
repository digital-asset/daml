// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.ledger.participant.state.v1.{AuthService, Claims}
import io.grpc.Metadata

/** An AuthService that authorizes all calls by always returning a wildcard [[Claims]] */
case object AuthServiceWildcard extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[Claims] = {
    CompletableFuture.completedFuture(Claims.wildcard)
  }
}