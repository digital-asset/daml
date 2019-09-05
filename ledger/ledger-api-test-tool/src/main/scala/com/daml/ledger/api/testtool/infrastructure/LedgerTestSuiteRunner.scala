// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Duration
import java.util.concurrent.{ExecutionException, Executors, TimeoutException}
import java.util.{Timer, TimerTask}

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite.SkipTestException
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuiteRunner.{
  TestTimeout,
  logger,
  timer
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionManager
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

object LedgerTestSuiteRunner {

  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestSuiteRunner])

  final class TestTimeout(testPromise: Promise[_], testDescription: String, testTimeoutMs: Long)
      extends TimerTask {
    override def run(): Unit = {
      if (testPromise.tryFailure(new TimeoutException())) {
        logger.error(s"Timeout of $testTimeoutMs ms for '$testDescription' hit")
      }
    }
  }

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private final case class UncaughtExceptionError(cause: Throwable)
      extends RuntimeException(uncaughtExceptionErrorMessage)

}

final class LedgerTestSuiteRunner(
    config: LedgerSessionConfiguration,
    suiteConstructors: Vector[LedgerSession => LedgerTestSuite],
    timeoutScaleFactor: Double,
    identifierSuffix: String) {

  private[this] val verifyRequirements: Try[Unit] =
    Try {
      require(timeoutScaleFactor > 0, "The timeout scale factor must be strictly positive")
      require(identifierSuffix.nonEmpty, "The identifier suffix cannot be an empty string")
    }

  private def start(test: LedgerTest, session: LedgerSession)(
      implicit ec: ExecutionContext): Future[Duration] = {
    val execution = Promise[Duration]
    val scaledTimeout = math.floor(test.timeout * timeoutScaleFactor).toLong
    val testTimeout = new TestTimeout(execution, test.description, scaledTimeout)
    val startedTest =
      session
        .createTestContext(test.shortIdentifier, identifierSuffix)
        .flatMap { context =>
          val start = System.nanoTime()
          val result = test(context).map(_ => Duration.ofNanos(System.nanoTime() - start))
          logger.info(s"Started '${test.description}' with a ${scaledTimeout} ms timeout!")
          result
        }
    timer.schedule(testTimeout, scaledTimeout)
    startedTest.onComplete { _ =>
      testTimeout.cancel()
      logger.info(s"Finished '${test.description}'")
    }
    execution.completeWith(startedTest).future
  }

  private def result(startedTest: Future[Duration])(implicit ec: ExecutionContext): Future[Result] =
    startedTest
      .map[Result](Result.Succeeded)
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
      }

  private def summarize(suite: LedgerTestSuite, test: LedgerTest, result: Future[Result])(
      implicit ec: ExecutionContext): Future[LedgerTestSummary] =
    result.map(LedgerTestSummary(suite.name, test.description, suite.session.config, _))

  private def run(test: LedgerTest, session: LedgerSession)(
      implicit ec: ExecutionContext): Future[Result] =
    result(start(test, session))

  private def run(suite: LedgerTestSuite)(
      implicit ec: ExecutionContext): Vector[Future[LedgerTestSummary]] =
    suite.tests.map(test => summarize(suite, test, run(test, suite.session)))

  private def run(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {
    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newSingleThreadExecutor(new Thread(_, s"test-tool-dispatcher")))
    val participantSessionManager = new ParticipantSessionManager
    Future {
      val ledgerSession = new LedgerSession(config, participantSessionManager)
      suiteConstructors.map(constructor => constructor(ledgerSession))
    }.flatMap(suites => Future.sequence(suites.flatMap(run)))
      .recover { case NonFatal(e) => throw LedgerTestSuiteRunner.UncaughtExceptionError(e) }
      .onComplete { result =>
        participantSessionManager.closeAll()
        completionCallback(result)
        ec.shutdown()
      }
  }

  def verifyRequirementsAndRun(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {
    verifyRequirements.fold(
      throwable => completionCallback(Failure(throwable)),
      _ => run(completionCallback)
    )
  }

}
