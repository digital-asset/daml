// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore.api

import com.daml.jwt.JwksUrl
import com.digitalasset.canton.ledger.api.IdentityProviderId

final case class IdentityProviderConfigUpdate(
    identityProviderId: IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[JwksUrl] = None,
    issuerUpdate: Option[String] = None,
    audienceUpdate: Option[Option[String]] = None,
)
