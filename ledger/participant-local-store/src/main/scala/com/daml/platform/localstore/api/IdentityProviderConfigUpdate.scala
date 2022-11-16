// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.lf.data.Ref

import java.net.URL

case class IdentityProviderConfigUpdate(
    identityProviderId: Ref.IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[URL],
    issuerUpdate: Option[String],
)
