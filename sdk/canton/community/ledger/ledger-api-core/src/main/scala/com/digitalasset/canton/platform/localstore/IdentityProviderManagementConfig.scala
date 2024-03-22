// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import com.digitalasset.canton.config.NonNegativeFiniteDuration

final case class IdentityProviderManagementConfig(
    cacheExpiryAfterWrite: NonNegativeFiniteDuration =
      IdentityProviderManagementConfig.DefaultCacheExpiryAfterWriteInSeconds
)
object IdentityProviderManagementConfig {
  val MaxIdentityProviders: Int = 16
  val DefaultCacheExpiryAfterWriteInSeconds: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(5)
}
