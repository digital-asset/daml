// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

class ScenarioLoadingITNewIdentifier extends ScenarioLoadingITBase {
  override def scenario: Option[String] = Some("Test:testScenario")
}
