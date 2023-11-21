// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

/** Ledger api user management service specific configurations
  *
  * @param enabled                        whether to enable participant user management
  * @param maxCacheSize                   maximum in-memory cache size for user management state
  * @param cacheExpiryAfterWriteInSeconds determines the maximum delay for propagating user management state changes
  * @param maxUsersPageSize               maximum number of users returned
  * @param maxRightsPerUser               maximum number of rights per user
  * @param additionalAdminUserId          adds an extra admin
  */
final case class UserManagementServiceConfig(
    enabled: Boolean = true,
    maxCacheSize: Int = UserManagementServiceConfig.DefaultMaxCacheSize,
    cacheExpiryAfterWriteInSeconds: Int =
      UserManagementServiceConfig.DefaultCacheExpiryAfterWriteInSeconds,
    maxUsersPageSize: Int = UserManagementServiceConfig.DefaultMaxUsersPageSize,
    maxRightsPerUser: Int = UserManagementServiceConfig.DefaultMaxRightsPerUser,
    additionalAdminUserId: Option[String] = None,
)

object UserManagementServiceConfig {

  val DefaultMaxCacheSize = 100
  val DefaultCacheExpiryAfterWriteInSeconds = 5
  val DefaultMaxUsersPageSize = 1000
  val DefaultMaxRightsPerUser = 1000

  def default(enabled: Boolean): UserManagementServiceConfig = UserManagementServiceConfig(
    enabled = enabled,
    maxCacheSize = DefaultMaxCacheSize,
    cacheExpiryAfterWriteInSeconds = DefaultCacheExpiryAfterWriteInSeconds,
    maxUsersPageSize = DefaultMaxUsersPageSize,
  )
}
