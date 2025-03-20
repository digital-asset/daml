// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.{CantonEdition, CommunityCantonEdition}
import com.digitalasset.canton.environment.{CommunityEnvironmentFactory, EnvironmentFactory}

trait CommunityIntegrationTest extends BaseIntegrationTest {
  this: EnvironmentSetup =>

  override val edition: CantonEdition = CommunityCantonEdition

  override protected val environmentFactory: EnvironmentFactory = CommunityEnvironmentFactory
}
