// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.persistence.PostgresAroundEach

class ScenarioLoadingITPostgres extends ScenarioLoadingITBase with PostgresAroundEach {
  override def config: SandboxConfig = super.config.copy(jdbcUrl = Some(postgresFixture.jdbcUrl))
  override def scenario: Option[String] = Some("Test:testScenario")
}
