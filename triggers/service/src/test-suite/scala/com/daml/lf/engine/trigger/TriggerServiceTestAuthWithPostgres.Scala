// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestAuthWithPostgresV1 extends TriggerServiceTestAuthWithPostgres(LanguageMajorVersion.V1)
class TriggerServiceTestAuthWithPostgresV2 extends TriggerServiceTestAuthWithPostgres(LanguageMajorVersion.V2)

class TriggerServiceTestAuthWithPostgres(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoPostgresFixture
    with AbstractTriggerServiceTestAuthMiddleware {}
