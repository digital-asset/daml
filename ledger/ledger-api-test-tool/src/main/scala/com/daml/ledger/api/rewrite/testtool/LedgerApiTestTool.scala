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

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION ON MAIN THREAD, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private def exitCode(summaries: Vector[LedgerTestSummary], expectFailure: Boolean): Int =
    if (summaries.exists(_.result.failure) == expectFailure) 0 else 1

  private def printAvailableTests(): Unit = {
    println("Tests marked with * are run by default.\n")
    tests.default.keySet.toSeq.sorted.map(_ + " *").foreach(println(_))
    tests.optional.keySet.toSeq.sorted.foreach(println(_))
  }

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    if (config.listTests) {
      printAvailableTests()
      sys.exit(0)
    }

    val included =
      if (config.allTests) tests.all.keySet
      else if (config.included.isEmpty) tests.default.keySet
      else config.included

    val testsToRun = tests.all.filterKeys(included -- config.excluded)

    if (testsToRun.isEmpty) {
      println("No tests to run.")
      sys.exit(0)
    }

    Thread
      .currentThread()
      .setUncaughtExceptionHandler((_, exception) => {
        logger.error(uncaughtExceptionErrorMessage, exception)
        sys.exit(1)
      })

    val runner = new LedgerTestSuiteRunner(
      Vector(LedgerSessionConfiguration(config.host, config.port, config.tlsConfig)),
      testsToRun.values.toVector,
      config.timeoutScaleFactor
    )

    runner.run {
      case Success(summaries) =>
        new ColorizedPrintStreamReporter(System.out, config.verbose)(summaries)
        sys.exit(exitCode(summaries, config.mustFail))
      case Failure(e) =>
        logger.error(e.getMessage, e)
        sys.exit(1)
    }
  }

}
