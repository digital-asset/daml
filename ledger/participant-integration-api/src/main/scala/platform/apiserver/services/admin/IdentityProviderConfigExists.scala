// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore

import scala.concurrent.Future

class IdentityProviderConfigExists(identityProviderConfigStore: IdentityProviderConfigStore) {
  def apply(id: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Boolean] = {
    id match {
      case IdentityProviderId.Default => Future.successful(true)
      case id: IdentityProviderId.Id =>
        identityProviderConfigStore.identityProviderConfigExists(id)
    }
  }
}
