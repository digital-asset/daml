// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

class TriggerServiceTestAuthClaims
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestInMem
    with AbstractTriggerServiceTestAuthMiddleware {
  override protected[this] def oauth2YieldsUserTokens = false
  override protected[this] def sandboxClientTakesUserToken = false
}
