// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.environment.CommunityEnvironment

object CommunityTests {

  type CommunityTestConsoleEnvironment = TestConsoleEnvironment[CommunityEnvironment]
  type CommunityIntegrationTest =
    BaseIntegrationTest[CommunityEnvironment, CommunityTestConsoleEnvironment]
  type SharedCommunityEnvironment =
    SharedEnvironment[CommunityEnvironment, CommunityTestConsoleEnvironment]
  type IsolatedCommunityEnvironments =
    IsolatedEnvironments[CommunityEnvironment, CommunityTestConsoleEnvironment]

}
