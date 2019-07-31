// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool

import com.daml.ledger.api.rewrite.testtool.infrastructure.LedgerTestSuiteRunner
import com.daml.ledger.api.rewrite.testtool.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

object LedgerApiTestTool {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {
    val tests = new LedgerTestSuiteRunner(Cli.parse(args).getOrElse(sys.exit(1))).run()

    tests.onComplete(result => {
      result match {
        case Success(results) =>
          new ColorizedPrintStreamReporter(System.out)(results)
          sys.exit(if (results.exists(_.result.failure)) 1 else 0)
        case Failure(e) =>
          logger.error("Unexpected uncaught exception, terminating!", e)
          sys.exit(1)
      }
    })(DirectExecutionContext)
  }

}
