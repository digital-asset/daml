// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.environment.{CommunityEnvironmentFactory, EnvironmentFactory}

trait CommunityIntegrationTest extends BaseIntegrationTest {
  this: EnvironmentSetup =>

  override protected val environmentFactory: EnvironmentFactory = CommunityEnvironmentFactory
}
