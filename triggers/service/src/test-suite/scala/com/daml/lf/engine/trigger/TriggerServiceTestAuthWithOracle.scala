// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestAuthWithOracleV1
    extends TriggerServiceTestAuthWithOracle(LanguageMajorVersion.V1)
class TriggerServiceTestAuthWithOracleV2
    extends TriggerServiceTestAuthWithOracle(LanguageMajorVersion.V2)

class TriggerServiceTestAuthWithOracle(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoOracleFixture
    with AbstractTriggerServiceTestAuthMiddleware
    with DisableOauthClaimsTests

class TriggerServiceTestAuthWithOracleClaimsV1
    extends TriggerServiceTestAuthWithOracleClaims(LanguageMajorVersion.V1)
class TriggerServiceTestAuthWithOracleClaimsV2
    extends TriggerServiceTestAuthWithOracleClaims(LanguageMajorVersion.V2)

class TriggerServiceTestAuthWithOracleClaims(
    override val majorLanguageVersion: LanguageMajorVersion
) extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoOracleFixture
    with AbstractTriggerServiceTestAuthMiddleware {
  protected[this] override def oauth2YieldsUserTokens = false
  protected[this] override def sandboxClientTakesUserToken = false
}
