// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.tests.examples.`ExampleIntegrationTest`.{
  jsonApiFolder,
  jsonApiTypescriptFolder,
}
import com.digitalasset.canton.util.ConcurrentBufferedLogger
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.sys.process.Process

class LedgerJsonApiDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      jsonApiFolder / "json.conf"
    )
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  private val processLogger = mkProcessLogger()
  private val dpmProcessLogger = mkProcessLogger(logErrors = false)

  "run the JSON API Bash demo" in { implicit env =>
    import env.*
    setupTest

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "./scenario.sh",
          participant1.config.httpLedgerApi.address + ":" + participant1.config.httpLedgerApi.port.unwrap.toString,
        ),
        cwd = jsonApiFolder.toJava,
      ),
      processLogger,
    )
  }

  "run the JSON API TypeScript example" in { implicit env =>
    setupTest
    import env.*
    setupTypescript()

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "npm",
          "run",
          "scenario",
        ),
        cwd = (jsonApiTypescriptFolder).toJava,
        ("PARTICIPANT_HOST", participant1.config.httpLedgerApi.address),
        ("PARTICIPANT_PORT", participant1.config.httpLedgerApi.port.unwrap.toString),
      ),
      processLogger,
    )

  }

  "run the JSON API TypeScript WebSocket example" in { implicit env =>
    setupTest
    import env.*
    setupTypescript()

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "npm",
          "run",
          "scenario_websocket",
        ),
        cwd = (jsonApiTypescriptFolder).toJava,
        ("PARTICIPANT_HOST", participant1.config.httpLedgerApi.address),
        ("PARTICIPANT_PORT", participant1.config.httpLedgerApi.port.unwrap.toString),
      ),
      processLogger,
    )
  }

  private def setupTypescript(): Unit = {
    runAndAssertCommandSuccess(
      Process(
        Seq(
          "npm",
          "install",
          "--loglevel",
          "error",
        ),
        cwd = (jsonApiTypescriptFolder).toJava,
      ),
      processLogger,
    )

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "dpm",
          "install",
          "package",
        ),
        cwd = (jsonApiFolder / "model").toJava,
        extraEnv = "DPM_REGISTRY" -> "europe-docker.pkg.dev/da-images/public-unstable",
      ),
      dpmProcessLogger,
    )

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "dpm",
          "codegen-js",
          ModelTestsPath,
          "-o",
          "../typescript/generated",
        ),
        cwd = (jsonApiFolder / "model").toJava,
      ),
      processLogger,
    )

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "npm",
          "run",
          "generate_api",
        ),
        cwd = (jsonApiTypescriptFolder).toJava,
      ),
      processLogger,
    )

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "npm",
          "run",
          "compile",
        ),
        cwd = (jsonApiTypescriptFolder).toJava,
      ),
      processLogger,
    )
  }

  private def setupTest(implicit env: FixtureParam): Unit = {
    import env.environment
    ExampleIntegrationTest.ensureSystemProperties("model-tests-examples.dar-path" -> ModelTestsPath)
    runScript(jsonApiFolder / "json.canton")(environment)
  }

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

}
