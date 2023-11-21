// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestWithPostgresV1
    extends TriggerServiceTestWithPostgres(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class TriggerServiceTestWithPostgresV2
    extends TriggerServiceTestWithPostgres(LanguageMajorVersion.V2)

class TriggerServiceTestWithPostgres(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoPostgresFixture
    with AbstractTriggerServiceTestNoAuth {}
