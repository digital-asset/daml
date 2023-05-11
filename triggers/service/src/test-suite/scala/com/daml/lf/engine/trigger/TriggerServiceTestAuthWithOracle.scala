// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

class TriggerServiceTestAuthWithOracle
    extends AbstractTriggerServiceTestWithCanton
    with AbstractTriggerServiceTestWithDatabaseAndCanton
    with TriggerDaoOracleCantonFixture
    with AbstractTriggerServiceTestAuthMiddlewareWithCanton
    with DisableOauthClaimsTestsWithCanton

class TriggerServiceTestAuthWithOracleClaims
    extends AbstractTriggerServiceTestWithCanton
    with AbstractTriggerServiceTestWithDatabaseAndCanton
    with TriggerDaoOracleCantonFixture
    with AbstractTriggerServiceTestAuthMiddlewareWithCanton {
  protected[this] override def oauth2YieldsUserTokens = false
  protected[this] override def sandboxClientTakesUserToken = false
}
