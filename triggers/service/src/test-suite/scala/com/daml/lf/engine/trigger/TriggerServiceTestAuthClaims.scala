// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestAuthClaimsV1 extends TriggerServiceTestAuthClaims(LanguageMajorVersion.V1)
class TriggerServiceTestAuthClaimsV2 extends TriggerServiceTestAuthClaims(LanguageMajorVersion.V2)

class TriggerServiceTestAuthClaims(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTestInMem
    with AbstractTriggerServiceTestAuthMiddleware {
  override protected[this] def oauth2YieldsUserTokens = false
  override protected[this] def sandboxClientTakesUserToken = false
}
