// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool

import com.daml.ledger.api.rewrite.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.api.rewrite.testtool.infrastructure.{
  LedgerSessionConfiguration,
  LedgerTestSuiteRunner,
  LedgerTestSummary
}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

object LedgerApiTestTool {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  private def exitCode(summaries: Vector[LedgerTestSummary], expectFailure: Boolean): Int =
    if (summaries.exists(_.result.failure) == expectFailure) 0 else 1

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    Thread
      .currentThread()
      .setUncaughtExceptionHandler((_, exception) => {
        logger.error("UNCAUGHT EXCEPTION ON MAIN THREAD", exception)
        sys.exit(1)
      })

    val runner = new LedgerTestSuiteRunner(
      Vector(
        LedgerSessionConfiguration(config.host, config.port, config.tlsConfig)
      ),
      tests.all.values.toVector
    )

    runner.run {
      case Success(summaries) =>
        new ColorizedPrintStreamReporter(System.out)(summaries)
        sys.exit(exitCode(summaries, config.mustFail))
      case Failure(e) =>
        logger.error(e.getMessage, e)
        sys.exit(1)
    }
  }

}
