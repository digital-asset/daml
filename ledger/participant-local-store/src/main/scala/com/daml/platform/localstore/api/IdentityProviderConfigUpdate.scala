// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.{IdentityProviderId, JwksUrl}

case class IdentityProviderConfigUpdate(
    identityProviderId: IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[JwksUrl] = None,
    issuerUpdate: Option[String] = None,
)
