// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.{File as BetterFile, *}
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.CommunityStorageConfig.Memory
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiDomain
import com.digitalasset.canton.integration.tests.DemoExampleIntegrationTest.referenceDemo

object DemoExampleIntegrationTest {
  lazy val referenceDemo: BetterFile = "community" / "demo" / "src" / "pack" / "demo"
}

sealed abstract class DemoExampleIntegrationTest
    extends ExampleIntegrationTest(referenceDemo / "demo.conf")
    with HasExecutionContext {

  "run reference demo" in { implicit env =>
    import env.*
    nodes.local.start()
    ExampleIntegrationTest.ensureSystemProperties(
      "demo-test" -> "1",
      "canton-demo.sync-timeout-seconds" -> "40", // in rare cases, the demo test flaked, but just because it was overloaded
    )
    runScript(referenceDemo / "demo.sc")(env.environment)
  }
}

final class DemoExampleReferenceIntegrationTest extends DemoExampleIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[Memory](
      loggerFactory,
      sequencerGroups = MultiDomain.tryCreate(Set("sequencerBanking"), Set("sequencerMedical")),
    )
  )
}
