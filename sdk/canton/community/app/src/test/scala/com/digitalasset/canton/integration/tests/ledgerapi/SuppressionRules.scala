// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.auth.{
  AuthInterceptor,
  AuthServiceJWT,
  AuthServicePrivilegedJWT,
  UserConfigAuthService,
}
import com.digitalasset.canton.ledger.api.auth.IdentityProviderAwareAuthService
import com.digitalasset.canton.logging.SuppressionRule
import org.slf4j.event.Level

object SuppressionRules {

  val AuthInterceptorSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[AuthInterceptor] && SuppressionRule.Level(Level.WARN)

  val AuthInterceptorAndJWTSuppressionRule: SuppressionRule =
    (SuppressionRule.forLogger[AuthInterceptor] || SuppressionRule
      .forLogger[AuthServiceJWT] || SuppressionRule
      .forLogger[AuthServicePrivilegedJWT]) && SuppressionRule.LevelAndAbove(Level.WARN)

  val IDPAndJWTSuppressionRule: SuppressionRule =
    (SuppressionRule.forLogger[IdentityProviderAwareAuthService] || SuppressionRule
      .forLogger[AuthServiceJWT] || SuppressionRule
      .forLogger[AuthServicePrivilegedJWT]) && SuppressionRule.LevelAndAbove(Level.WARN)

  val AuthServiceJWTSuppressionRule: SuppressionRule =
    (SuppressionRule.forLogger[AuthServiceJWT]
      || SuppressionRule.forLogger[AuthServicePrivilegedJWT]
      || SuppressionRule.forLogger[UserConfigAuthService])
      && SuppressionRule.LevelAndAbove(Level.WARN)

  val ApiUserManagementServiceSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiUserManagementService") &&
      SuppressionRule.Level(Level.WARN)

  val ApiPartyManagementServiceSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiPartyManagementService") &&
      SuppressionRule.Level(Level.WARN)

}
