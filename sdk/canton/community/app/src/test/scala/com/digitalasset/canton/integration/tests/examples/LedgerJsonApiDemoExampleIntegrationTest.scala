// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.File
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.JsonApiExample
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.sys.process.Process

class LedgerJsonApiDemoExampleIntegrationTest
    extends ExampleIntegrationTest(JsonApiExample.path / "json.conf")
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  private val processLogger = mkProcessLogger()

  "run the JSON API Bash demo" in { implicit env =>
    import env.*
    setupTest

    runAndAssertCommandSuccess(
      Process(
        Seq(
          "./scenario.sh",
          participant1.config.httpLedgerApi.address + ":" + participant1.config.httpLedgerApi.port.unwrap.toString,
        ),
        cwd = JsonApiExample.path.toJava,
      ),
      processLogger,
    )
  }

  "run the JSON API TypeScript example" in { implicit env =>
    setupTest
    import env.*
    usingTypescript { tsDir =>
      runAndAssertCommandSuccess(
        Process(
          Seq("npm", "run", "scenario"),
          cwd = tsDir.toJava,
          ("PARTICIPANT_HOST", participant1.config.httpLedgerApi.address),
          ("PARTICIPANT_PORT", participant1.config.httpLedgerApi.port.unwrap.toString),
        ),
        processLogger,
      )
    }
  }

  "run the JSON API TypeScript WebSocket example" in { implicit env =>
    setupTest
    import env.*
    usingTypescript { tsDir =>
      runAndAssertCommandSuccess(
        Process(
          Seq("npm", "run", "scenario_websocket"),
          cwd = tsDir.toJava,
          ("PARTICIPANT_HOST", participant1.config.httpLedgerApi.address),
          ("PARTICIPANT_PORT", participant1.config.httpLedgerApi.port.unwrap.toString),
        ),
        processLogger,
      )
    }
  }

  private def usingTypescript(f: File => Unit): Unit =
    File.usingTemporaryDirectory("LedgerJsonApiDemoExampleIntegrationTest") { tempDir =>
      (JsonApiExample.path / "typescript").copyTo(tempDir)
      JsonApiExample.codegenOutput.copyTo(tempDir / "generated")

      runAndAssertCommandSuccess(
        Process(
          Seq("npm", "install", "--loglevel", "error"),
          cwd = tempDir.toJava,
        ),
        processLogger,
      )

      runAndAssertCommandSuccess(
        Process(
          Seq("npm", "run", "generate_api"),
          cwd = tempDir.toJava,
        ),
        processLogger,
      )

      runAndAssertCommandSuccess(
        Process(
          Seq("npm", "run", "compile"),
          cwd = tempDir.toJava,
        ),
        processLogger,
      )

      f(tempDir)
    }

  private def setupTest(implicit env: FixtureParam): Unit = {
    import env.environment
    ExampleIntegrationTest.ensureSystemProperties("model-tests-examples.dar-path" -> ModelTestsPath)
    runScript(JsonApiExample.path / "json.canton")(environment)
  }

}
