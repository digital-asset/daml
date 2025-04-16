// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.File
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.examplesPath
import com.digitalasset.canton.integration.tests.examples.OpenSslOfflineRootKeyDemoExampleIntegrationTest.demoFolder
import com.digitalasset.canton.util.ConcurrentBufferedLogger
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.sys.process.Process

object OpenSslOfflineRootKeyDemoExampleIntegrationTest {
  lazy val demoFolder: File = examplesPath / "10-offline-root-namespace-init"
}

sealed abstract class OpenSslOfflineRootKeyDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      demoFolder / "external-init-example.conf"
    )
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  private def mkProcessLogger(logErrors: Boolean = true) = new ConcurrentBufferedLogger {
    override def out(s: => String): Unit = {
      logger.info(s)
      super.out(s)
    }
    override def err(s: => String): Unit = {
      if (logErrors) logger.error(s)
      super.err(s)
    }
  }
  private val processLogger = mkProcessLogger()

  override def afterAll(): Unit = {
    super.afterAll()
    // Delete the temp files created by the test
    (demoFolder / "tmp").delete(swallowIOExceptions = true)
  }

  "run offline root namespace key init demo" in { implicit env =>
    import env.*

    val tmpDir = File.newTemporaryDirectory()
    val delegateKeyPath = (tmpDir / "delegate_key.pub").pathAsString
    // Generate delegate key
    val delegateKey = participant1.keys.secret.generate_signing_key(
      name = "NamespaceDelegation",
      usage = com.digitalasset.canton.crypto.SigningKeyUsage.NamespaceOnly,
    )
    // Write public key to file
    participant1.keys.public
      .download_to(delegateKey.id, delegateKeyPath)

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "./openssl-example.sh",
          delegateKeyPath,
        ),
        cwd = demoFolder.toJava,
        extraEnv = "OUTPUT_DIR" -> tmpDir.pathAsString,
      ),
      processLogger,
    )

    participant1.is_initialized shouldBe false

    participant1.topology.init_id(
      identifier = "participant1",
      delegationFiles = Seq(
        (tmpDir / "root_namespace.cert").pathAsString,
        (tmpDir / "intermediate_namespace.cert").pathAsString,
      ),
    )

    participant1.is_initialized shouldBe true
  }
}

final class OpenSslOfflineRootKeyDemoExampleIntegrationTestH2
    extends OpenSslOfflineRootKeyDemoExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
