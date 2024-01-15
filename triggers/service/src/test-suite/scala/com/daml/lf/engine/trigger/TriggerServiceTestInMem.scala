// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.language.LanguageMajorVersion

class TriggerServiceTestInMemV1 extends TriggerServiceTestInMem(LanguageMajorVersion.V1)
class TriggerServiceTestInMemV2 extends TriggerServiceTestInMem(LanguageMajorVersion.V2)

class TriggerServiceTestInMem(override val majorLanguageVersion: LanguageMajorVersion)
    extends AbstractTriggerServiceTestInMem
    with AbstractTriggerServiceTestNoAuth {}
