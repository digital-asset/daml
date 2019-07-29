// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance

import java.io.File

import com.daml.ledger.acceptance.infrastructure.Reporter.ColorizedPrintStreamReporter
import com.daml.ledger.acceptance.infrastructure.{
  LedgerSession,
  LedgerSessionConfiguration,
  LedgerTestSuiteRunner
}
import com.daml.ledger.acceptance.tests.{Divulgence, Identity}
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType.WallClock
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Main {

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def main(args: Array[String]): Unit = {

    val testDar = new File(BazelRunfiles.rlocation("ledger/sandbox/Test.dar"))

    val configurations = Vector(
      LedgerSessionConfiguration.Managed(
        SandboxConfig.default
          .copy(port = 0, damlPackages = List(testDar), timeProviderType = WallClock))
    )

    val sessions =
      configurations.map(LedgerSession.getOrCreate).foldLeft(Vector.empty[LedgerSession]) {
        (sessions, attempt) =>
          attempt match {
            case Failure(NonFatal(cause)) =>
              logger.error(s"A managed ledger session could not be started, quitting...", cause)
              sys.exit(1)
            case Success(session) => sessions :+ session
          }
      }

    val suiteConstructors = Vector(new Divulgence(_), new Identity(_))

    val testSuites =
      for (session <- sessions; suiteConstructor <- suiteConstructors)
        yield suiteConstructor(session)

    implicit val ec: ExecutionContext = ExecutionContext.global
    val runner = new LedgerTestSuiteRunner(ec)

    Future.sequence(testSuites.flatMap(runner.run)).onComplete {
      case Success(results) =>
        LedgerSession.closeAll()
        new ColorizedPrintStreamReporter(System.out)(results)
      case Failure(e) =>
        LedgerSession.closeAll()
        logger.error("Unexpected uncaught exception, terminating!", e)
        sys.exit(1)
    }

  }

}
