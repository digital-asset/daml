// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.File
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.examplesPath
import com.digitalasset.canton.integration.tests.examples.OpenSslOfflineRootKeyDemoExampleIntegrationTest.demoFolder
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

object OpenSslOfflineRootKeyDemoExampleIntegrationTest {
  lazy val demoFolder: File = examplesPath / "10-offline-root-namespace-init"
}

sealed abstract class OpenSslOfflineRootKeyDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      demoFolder / "external-init-example.conf"
    )
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  override def afterAll(): Unit = {
    super.afterAll()
    // Delete the temp files created by the test
    (demoFolder / "tmp").delete(swallowIOExceptions = true)
  }

  "run offline root namespace key init demo" in { implicit env =>
    import env.*

    ExampleIntegrationTest.ensureSystemProperties(
      "canton-examples.openssl-script-dir" -> demoFolder.pathAsString
    )
    runScript(demoFolder / "bootstrap.canton")(environment)
    participant1.is_initialized shouldBe true
  }
}

final class OpenSslOfflineRootKeyDemoExampleIntegrationTestH2
    extends OpenSslOfflineRootKeyDemoExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
