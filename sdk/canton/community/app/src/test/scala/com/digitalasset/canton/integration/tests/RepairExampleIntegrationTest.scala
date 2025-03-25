// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.{UseCommunityReferenceBlockSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ExampleIntegrationTest.{
  referenceConfiguration,
  repairConfiguration,
}

sealed abstract class RepairExampleIntegrationTest
    extends ExampleIntegrationTest(
      referenceConfiguration / "storage" / "h2.conf",
      repairConfiguration / "synchronizer-repair-lost.conf",
      repairConfiguration / "synchronizer-repair-new.conf",
      repairConfiguration / "participant1.conf",
      repairConfiguration / "participant2.conf",
      repairConfiguration / "enable-preview-commands.conf",
    )
    with CommunityIntegrationTest {
  "deploy repair user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties("canton-examples.dar-path" -> CantonExamplesPath)
    runScript(repairConfiguration / "synchronizer-repair-init.canton")(env.environment)
  }
}

final class RepairExampleReferenceIntegrationTestH2 extends RepairExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
