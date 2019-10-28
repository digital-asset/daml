// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.digitalasset.ledger.api.auth.{AuthService, Claims}
import io.grpc.Metadata

/** An AuthService that rejects all calls by always returning an empty set of [[Claims]] */
object AuthServiceNone extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[Claims] = {
    CompletableFuture.completedFuture(Claims.empty)
  }
}