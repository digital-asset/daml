// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.tests.examples.`ExampleIntegrationTest`.jsonApiFolder
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
