// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.lf.language.LanguageMajorVersion
import com.daml.platform.services.time.TimeProviderType

class FuncTestsStaticTimeV1 extends FuncTestsStaticTime(LanguageMajorVersion.V1)
class FuncTestsStaticTimeV2 extends FuncTestsStaticTime(LanguageMajorVersion.V2)

class FuncTestsStaticTime(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractFuncTests {

  final override protected lazy val timeProviderType: TimeProviderType = TimeProviderType.Static

}
