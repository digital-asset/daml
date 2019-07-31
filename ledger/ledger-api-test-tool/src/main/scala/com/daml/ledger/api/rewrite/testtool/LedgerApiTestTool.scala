// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool

import com.daml.ledger.api.rewrite.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.api.rewrite.testtool.infrastructure.{
  LedgerSessionConfiguration,
  LedgerTestSuiteRunner
}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

object LedgerApiTestTool {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    val runner = new LedgerTestSuiteRunner(
      Vector(LedgerSessionConfiguration(config.host, config.port)),
      tests.all.values.toVector
    )

    runner.run {
      case Success(summaries) =>
        new ColorizedPrintStreamReporter(System.out)(summaries)
        sys.exit(if (summaries.exists(_.result.failure)) 1 else 0)
      case Failure(e) =>
        logger.error("Unexpected uncaught exception, terminating!", e)
        sys.exit(1)
    }
  }

}
