// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.digitalasset.canton.ledger.api.domain.IdentityProviderConfig
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait IdentityProviderConfigLoader {

  def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[IdentityProviderConfig]

}
