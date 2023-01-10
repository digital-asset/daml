// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import scala.concurrent.duration.FiniteDuration

case class IdentityProviderManagementConfig(
    cacheExpiryAfterWrite: FiniteDuration =
      IdentityProviderManagementConfig.DefaultCacheExpiryAfterWriteInSeconds
)
object IdentityProviderManagementConfig {
  import scala.concurrent.duration._
  val MaxIdentityProviders: Int = 16
  val DefaultCacheExpiryAfterWriteInSeconds: FiniteDuration = 5.minutes
}
