// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release.kms

import better.files.File
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.plugins.UseExternalConsole
import com.digitalasset.canton.integration.tests.release.ReleaseArtifactIntegrationTestUtils
import org.scalatest.Outcome

/** Cli integration test for KMS configurations.
  *
  * Before being able to run these tests locally, you need to execute `sbt bundle`.
  */
trait KmsCliIntegrationTest extends ReleaseArtifactIntegrationTestUtils {

  protected def kmsConfigs: Seq[String]
  protected def cantonProcessEnvVar: Seq[(String, String)]
  protected def bootstrapScript: String
  protected def testName: String

  override protected val isEnterprise: Boolean = false
  override protected def withFixture(test: OneArgTest): Outcome = test(new BufferedProcessLogger)

  override type FixtureParam = BufferedProcessLogger

  private lazy val simpleConf =
    "community/app/src/pack/examples/01-simple-topology/simple-topology.conf"

  private lazy val kmsLog = s"log/$testName-kms.log"

  private def createExternalConsole(
      extraConfigArguments: Seq[String] = Seq.empty
  ): UseExternalConsole =
    UseExternalConsole(
      remoteConfigOrConfigFile = Right(File(simpleConf)),
      cantonBin = cantonBin,
      extraConfigs = kmsConfigs.map(c => s"--config $c") ++ extraConfigArguments,
      extraEnv = cantonProcessEnvVar,
      fileNameHint = s"${this.getClass.getSimpleName}-$testName",
    )

  "Calling Canton" should {

    "bootstrap and run ping" in { processLogger =>
      val externalConsole = createExternalConsole()
      externalConsole.runBootstrapScript(bootstrapScript)
      checkOutput(processLogger)
    }

    "send kms audit logs to a separate file" in { processLogger =>
      // ensure kms log is empty
      File(kmsLog).clear()

      val externalConsole = createExternalConsole(Seq(s"--kms-log-file-name $kmsLog"))

      externalConsole.runBootstrapScript(bootstrapScript)

      checkOutput(processLogger)
      val logFile = File(externalConsole.logFile)
      assert(logFile.exists)
      val contents = logFile.contentAsString
      assert(!contents.contains(": GetPublicKeyRequest"))
      val kmsLogFile = File(kmsLog)
      assert(kmsLogFile.exists)
      val kmsContents = kmsLogFile.contentAsString
      assert(kmsContents.contains(": GetPublicKeyRequest"))
    }

    "merge kms logging with canton file by default" in { processLogger =>
      val externalConsole = createExternalConsole()

      externalConsole.runBootstrapScript(bootstrapScript)

      val logFile = File(externalConsole.logFile)
      checkOutput(processLogger)
      assert(logFile.exists)
      val contents = logFile.contentAsString
      assert(contents.contains(": GetPublicKeyRequest"))
    }
  }
}
