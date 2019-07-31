// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import java.util.concurrent.{ExecutionException, Executors, TimeoutException}
import java.util.{Timer, TimerTask}

import com.daml.ledger.api.rewrite.testtool.Config
import com.daml.ledger.api.rewrite.testtool.infrastructure.LedgerTestSuite.SkipTestException
import com.daml.ledger.api.rewrite.testtool.infrastructure.LedgerTestSuiteRunner.{
  Timeout,
  logger,
  timer
}
import com.daml.ledger.api.rewrite.testtool.tests.{Divulgence, Identity}
import com.digitalasset.grpc.adapter.utils.{DirectExecutionContext => parasitic}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object LedgerTestSuiteRunner {

  final class Timeout[A](p: Promise[A]) extends TimerTask {
    override def run(): Unit = p.failure(new TimeoutException())
  }

  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestSuiteRunner])

}

final class LedgerTestSuiteRunner(config: Config) {

  private[this] val timeoutMillis: Long = 30000L

  private def start(test: LedgerTest, session: LedgerSession): Future[Unit] = {
    val execution = Promise[Unit]()
    val timeout = new Timeout(execution)
    timer.schedule(timeout, timeoutMillis)
    logger.info(s"Started $timeoutMillis ms timeout for '${test.description}'...")
    val startedTest = test(session.createTestContext())
    logger.info(s"Started '${test.description}'!")
    startedTest.onComplete { _ =>
      logger.info(s"Finished '${test.description}")
      timeout.cancel()
    }(parasitic)
    execution.completeWith(startedTest).future
  }

  private def result(startedTest: Future[Unit]): Future[Result] =
    startedTest
      .map[Result](_ => Result.Succeeded)(parasitic)
      .recover[Result] {
        case SkipTestException(reason) =>
          Result.Skipped(reason)
        case _: TimeoutException =>
          Result.TimedOut
        case failure: AssertionError =>
          Result.Failed(failure)
        case NonFatal(box: ExecutionException) =>
          box.getCause() match {
            case failure: AssertionError =>
              Result.Failed(failure)
            case NonFatal(exception) =>
              Result.FailedUnexpectedly(exception)
          }
        case NonFatal(exception) =>
          Result.FailedUnexpectedly(exception)
      }(parasitic)

  private def summarize(
      suite: LedgerTestSuite,
      test: LedgerTest,
      result: Future[Result]): Future[LedgerTestSummary] =
    result.map(LedgerTestSummary(suite.name, test.description, suite.session.config, _))(parasitic)

  private def run(test: LedgerTest, session: LedgerSession): Future[Result] =
    result(start(test, session))

  private def run(suite: LedgerTestSuite): Vector[Future[LedgerTestSummary]] =
    suite.tests.map(test => summarize(suite, test, run(test, suite.session)))

  def run(): Future[Vector[LedgerTestSummary]] = {

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

    // TODO should pick up from cli params
    val suiteConstructors = Vector(new Divulgence(_), new Identity(_))

    val testSuites =
      for (session <- sessions; suiteConstructor <- suiteConstructors)
        yield suiteConstructor(session)

    for {
      startedTests <- Future.sequence(testSuites.flatMap(run))
      _ = LedgerSession.closeAll()
    } yield startedTests
  }

}
