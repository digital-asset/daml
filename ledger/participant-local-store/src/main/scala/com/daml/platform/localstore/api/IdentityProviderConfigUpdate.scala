// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.JwksUrl
import com.daml.lf.data.Ref

case class IdentityProviderConfigUpdate(
    identityProviderId: Ref.IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[JwksUrl],
    issuerUpdate: Option[String],
)
