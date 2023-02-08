// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.interceptor

import com.daml.ledger.api.auth.ClaimSet
import io.grpc.Metadata

import scala.concurrent.Future

trait IdentityProviderAwareAuthService {
  def decodeMetadata(headers: Metadata): Future[ClaimSet]
}
