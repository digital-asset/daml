// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.{CantonEdition, CommunityCantonEdition}

object CommunityTests {

  type CommunityTestConsoleEnvironment = TestConsoleEnvironment

  trait CommunityIntegrationTest extends BaseIntegrationTest[CommunityTestConsoleEnvironment] {
    this: EnvironmentSetup[CommunityTestConsoleEnvironment] =>

    override val edition: CantonEdition = CommunityCantonEdition
  }

  type SharedCommunityEnvironment =
    SharedEnvironment[CommunityTestConsoleEnvironment]
  type IsolatedCommunityEnvironments =
    IsolatedEnvironments[CommunityTestConsoleEnvironment]

}
