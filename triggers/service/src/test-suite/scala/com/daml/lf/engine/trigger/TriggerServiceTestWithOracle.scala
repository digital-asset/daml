// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestWithOracleV1 extends TriggerServiceTestWithOracle(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
class TriggerServiceTestWithOracleV2 extends TriggerServiceTestWithOracle(LanguageMajorVersion.V2)

class TriggerServiceTestWithOracle(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoOracleFixture
    with AbstractTriggerServiceTestNoAuth {}
