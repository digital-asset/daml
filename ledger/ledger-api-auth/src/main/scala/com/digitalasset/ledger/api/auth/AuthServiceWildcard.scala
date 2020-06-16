// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.logging.ThreadLogger
import io.grpc.Metadata

/** An AuthService that authorizes all calls by always returning a wildcard [[Claims]] */
object AuthServiceWildcard extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[Claims] = {
    ThreadLogger.traceThread("AuthServiceWildcard.decodeMetadata")
    CompletableFuture.completedFuture(Claims.wildcard)
  }
}
