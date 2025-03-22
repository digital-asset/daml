// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.ExampleIntegrationTest.composabilityConfiguration

abstract class ComposabilityExampleIntegrationTest
    extends ExampleIntegrationTest(composabilityConfiguration / "composability.conf")
    with CommunityIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))

  "Canton" should {
    forEvery(
      Seq(
        "composability1.canton",
        "composability-auto-reassignment.canton",
        "composability2.canton",
      ).zipWithIndex
    ) { case (script, idx) =>
      s"run $script successfully" in { implicit env =>
        ExampleIntegrationTest.ensureSystemProperties(
          "canton-examples.dar-path" -> CantonExamplesPath
        )
        if (idx > 0) {
          // Since the nodes use in-memory storage, stopping and restarting completely resets them.
          env.nodes.local.foreach { n =>
            n.stop()
            n.start()
          }
        }
        runScript(composabilityConfiguration / script)(env.environment)
      }
    }
  }
}

final class ComposabilityExampleIntegrationTestPostgres
    extends ComposabilityExampleIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
