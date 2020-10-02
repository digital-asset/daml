// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import io.grpc.Metadata

/** An AuthService that rejects all calls by always returning an empty set of [[Claims]] */
object AuthServiceNone extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[Claims] = {
    CompletableFuture.completedFuture(Claims.empty)
  }
}
