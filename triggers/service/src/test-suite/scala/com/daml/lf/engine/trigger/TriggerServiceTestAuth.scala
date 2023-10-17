// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestAuthV1 extends TriggerServiceTestAuth(LanguageMajorVersion.V1)
class TriggerServiceTestAuthV2 extends TriggerServiceTestAuth(LanguageMajorVersion.V2)

class TriggerServiceTestAuth(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTestInMem
    with AbstractTriggerServiceTestAuthMiddleware
    with DisableOauthClaimsTests
