// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.localstore.api.IdentityProviderConfigStore

import scala.concurrent.Future

class IdentityProviderExists(identityProviderConfigStore: IdentityProviderConfigStore) {
  def apply(id: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Boolean] =
    id match {
      case IdentityProviderId.Default => Future.successful(true)
      case id: IdentityProviderId.Id =>
        identityProviderConfigStore.identityProviderConfigExists(id)
    }
}
