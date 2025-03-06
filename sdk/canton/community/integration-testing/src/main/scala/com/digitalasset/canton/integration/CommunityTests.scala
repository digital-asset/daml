// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.{CantonEdition, CommunityCantonEdition}
import com.digitalasset.canton.environment.CommunityEnvironment

object CommunityTests {

  type CommunityTestConsoleEnvironment = TestConsoleEnvironment[CommunityEnvironment]

  trait CommunityIntegrationTest
      extends BaseIntegrationTest[CommunityEnvironment, CommunityTestConsoleEnvironment] {
    this: EnvironmentSetup[CommunityEnvironment, CommunityTestConsoleEnvironment] =>

    override val edition: CantonEdition = CommunityCantonEdition
  }

  type SharedCommunityEnvironment =
    SharedEnvironment[CommunityEnvironment, CommunityTestConsoleEnvironment]
  type IsolatedCommunityEnvironments =
    IsolatedEnvironments[CommunityEnvironment, CommunityTestConsoleEnvironment]

}
