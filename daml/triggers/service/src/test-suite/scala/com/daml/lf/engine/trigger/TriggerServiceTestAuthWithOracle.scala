// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

class TriggerServiceTestAuthWithOracle
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoOracleFixture
    with AbstractTriggerServiceTestAuthMiddleware
    with DisableOauthClaimsTests

class TriggerServiceTestAuthWithOracleClaims
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoOracleFixture
    with AbstractTriggerServiceTestAuthMiddleware {
  protected[this] override def oauth2YieldsUserTokens = false
  protected[this] override def sandboxClientTakesUserToken = false
}
