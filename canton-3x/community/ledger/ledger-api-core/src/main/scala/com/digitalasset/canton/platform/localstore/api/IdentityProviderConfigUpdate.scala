// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, JwksUrl}

final case class IdentityProviderConfigUpdate(
    identityProviderId: IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[JwksUrl] = None,
    issuerUpdate: Option[String] = None,
    audienceUpdate: Option[Option[String]] = None,
)
