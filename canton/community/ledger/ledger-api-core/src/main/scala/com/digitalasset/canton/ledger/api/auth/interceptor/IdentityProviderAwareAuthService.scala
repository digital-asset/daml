// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.interceptor

import com.digitalasset.canton.ledger.api.auth.ClaimSet
import com.digitalasset.canton.logging.LoggingContextWithTrace
import io.grpc.Metadata

import scala.concurrent.Future

trait IdentityProviderAwareAuthService {
  def decodeMetadata(headers: Metadata)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ClaimSet]
}
