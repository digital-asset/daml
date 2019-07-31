// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance

import java.util.concurrent.Executors

import com.daml.ledger.acceptance.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.acceptance.infrastructure.{
  LedgerSession,
  LedgerSessionConfiguration,
  LedgerTestSuiteRunner
}
import com.daml.ledger.acceptance.tests.{Divulgence, Identity}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Main {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {

    val config = Cli.parse(args).getOrElse(sys.exit(1))

    implicit val ec: ExecutionContext = {
      val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
      val _ = sys.addShutdownHook { val _ = ec.shutdownNow() }
      ec
    }

    val configurations = Vector(
      LedgerSessionConfiguration(config.host, config.port)
    )

    val sessions =
      configurations.map(LedgerSession.getOrCreate).foldLeft(Vector.empty[LedgerSession]) {
        (sessions, attempt) =>
          attempt match {
            case Failure(NonFatal(cause)) =>
              logger.error(s"A ledger session could not be started, quitting...", cause)
              sys.exit(1)
            case Success(session) => sessions :+ session
          }
      }

    val suiteConstructors = Vector(new Divulgence(_), new Identity(_))

    val testSuites =
      for (session <- sessions; suiteConstructor <- suiteConstructors)
        yield suiteConstructor(session)
    val runner = new LedgerTestSuiteRunner(ec)
    val startedTests = Future.sequence(testSuites.flatMap(runner.run))

    startedTests.onComplete(result => {
      LedgerSession.closeAll()
      result match {
        case Success(results) =>
          new ColorizedPrintStreamReporter(System.out)(results)
          sys.exit(if (results.exists(_.result.failure)) 1 else 0)
        case Failure(e) =>
          logger.error("Unexpected uncaught exception, terminating!", e)
          sys.exit(1)
      }
    })

  }

}
