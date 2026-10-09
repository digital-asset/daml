// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.sequencer.DynamicOnboardingIntegrationTest

abstract class ReferenceDynamicOnboardingIntegrationTestBase(driverName: String)
    extends DynamicOnboardingIntegrationTest(driverName) {

  protected def plugin: EnvironmentSetupPlugin

  registerPlugin(plugin)
  registerPlugin(new UsePostgres(loggerFactory))
}
