// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import java.util.concurrent.{ExecutionException, Executors, TimeoutException}
import java.util.{Timer, TimerTask}

import com.daml.ledger.api.rewrite.testtool.infrastructure.LedgerTestSuite.SkipTestException
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object LedgerTestSuiteRunner {

  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestSuiteRunner])

  final class TestTimeout(
      testPromise: Promise[_],
      testDescription: String,
      testTimeoutMs: Long,
      sessionConfig: LedgerSessionConfiguration)
      extends TimerTask {
    override def run(): Unit = {
      if (testPromise.tryFailure(new TimeoutException())) {
        val LedgerSessionConfiguration(host, port, _) = sessionConfig
        logger.error(s"Timeout of $testTimeoutMs ms for '$testDescription' hit ($host:$port)")
      }
    }
  }

  private def start(test: LedgerTest, session: LedgerSession)(
      implicit ec: ExecutionContext): Future[Unit] = {
    val execution = Promise[Unit]
    val testTimeout = new TestTimeout(execution, test.description, test.timeout, session.config)
    timer.schedule(testTimeout, test.timeout)
    logger.info(s"Started ${test.timeout} ms timeout for '${test.description}'...")
    val startedTest = Future(session.createTestContext()).flatMap(test)
    logger.info(s"Started '${test.description}'!")
    startedTest.onComplete { _ =>
      testTimeout.cancel()
      logger.info(s"Finished '${test.description}")
    }
    execution.completeWith(startedTest).future
  }

  private def result(startedTest: Future[Unit])(implicit ec: ExecutionContext): Future[Result] =
    startedTest
      .map[Result](_ => Result.Succeeded)
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

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private final case class UncaughtExceptionError(cause: Throwable)
      extends RuntimeException(uncaughtExceptionErrorMessage)

}

final class LedgerTestSuiteRunner(
    endpoints: Vector[LedgerSessionConfiguration],
    suiteConstructors: Vector[LedgerSession => LedgerTestSuite]) {

  private def initSessions()(implicit ec: ExecutionContext): Try[Vector[LedgerSession]] = {
    @tailrec
    def go(
        conf: Vector[LedgerSessionConfiguration],
        result: Try[Vector[LedgerSession]]): Try[Vector[LedgerSession]] = {
      (conf, result) match {
        case (remaining, result) if remaining.isEmpty || result.isFailure => result
        case (endpoint +: remaining, Success(sessions)) =>
          val newSession = LedgerSession.getOrCreate(endpoint)
          val newResult = newSession.map(sessions :+ _)
          go(remaining, newResult)
      }
    }
    go(endpoints, Try(Vector.empty))
  }

  private def initTestSuites(sessions: Vector[LedgerSession]): Vector[LedgerTestSuite] =
    for (session <- sessions; suiteConstructor <- suiteConstructors)
      yield suiteConstructor(session)

  def run(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {

    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

    def cleanUpAndComplete(result: Try[Vector[LedgerTestSummary]]): Unit = {
      ec.shutdown()
      endpoints.foreach(LedgerSession.close)
      completionCallback(result)
    }

    initSessions() match {

      case Failure(exception) =>
        cleanUpAndComplete(Failure(exception))

      case Success(sessions) =>
        Future
          .sequence(initTestSuites(sessions).flatMap(LedgerTestSuiteRunner.run))
          .recover { case NonFatal(e) => throw LedgerTestSuiteRunner.UncaughtExceptionError(e) }
          .onComplete(cleanUpAndComplete)

    }

  }

}
