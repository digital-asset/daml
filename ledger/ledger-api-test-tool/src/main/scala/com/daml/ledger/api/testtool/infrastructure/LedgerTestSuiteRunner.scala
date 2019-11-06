// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.{ExecutionException, Executors, TimeoutException}
import java.util.{Timer, TimerTask}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite.SkipTestException
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuiteRunner._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionManager
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object LedgerTestSuiteRunner {
  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestSuiteRunner])

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private final case class UncaughtExceptionError(cause: Throwable)
      extends RuntimeException(uncaughtExceptionErrorMessage)
}

final class LedgerTestSuiteRunner(
    config: LedgerSessionConfiguration,
    suiteConstructors: Vector[LedgerSession => LedgerTestSuite],
    identifierSuffix: String,
    timeoutScaleFactor: Double,
    concurrentTestRuns: Int) {
  private[this] val verifyRequirements: Try[Unit] =
    Try {
      require(timeoutScaleFactor > 0, "The timeout scale factor must be strictly positive")
      require(identifierSuffix.nonEmpty, "The identifier suffix cannot be an empty string")
    }

  private def start(test: LedgerTestCase, session: LedgerSession)(
      implicit ec: ExecutionContext): Future[Duration] = {
    val execution = Promise[Duration]
    val scaledTimeout = test.timeout * timeoutScaleFactor

    val startedTest =
      session
        .createTestContext(test.shortIdentifier, identifierSuffix)
        .flatMap { context =>
          val start = System.nanoTime()
          val result = test(context).map(_ => Duration.fromNanos(System.nanoTime() - start))
          logger.info(s"Started '${test.description}' with a ${scaledTimeout.toMillis}ms timeout.")
          result
        }

    val testTimeout = new TimerTask {
      override def run(): Unit = {
        val message = s"Timeout of ${scaledTimeout.toMillis}ms for '${test.description}' hit."
        if (execution.tryFailure(new TimeoutException(message))) {
          logger.error(message)
        }
      }
    }
    timer.schedule(testTimeout, scaledTimeout.toMillis)
    startedTest.onComplete { _ =>
      testTimeout.cancel()
      logger.info(s"Finished '${test.description}'.")
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
          box.getCause match {
            case failure: AssertionError =>
              Result.Failed(failure)
            case NonFatal(exception) =>
              Result.FailedUnexpectedly(exception)
          }
        case NonFatal(exception) =>
          Result.FailedUnexpectedly(exception)
      }

  private def summarize(suite: LedgerTestSuite, test: LedgerTestCase, result: Result)(
      implicit ec: ExecutionContext): LedgerTestSummary =
    LedgerTestSummary(suite.name, test.description, suite.session.config, result)

  private def run(test: LedgerTestCase, session: LedgerSession)(
      implicit ec: ExecutionContext): Future[Result] =
    result(start(test, session))

  private def run(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    val system: ActorSystem =
      ActorSystem(
        classOf[LedgerTestSuiteRunner].getSimpleName,
        defaultExecutionContext = Some(executionContext))
    implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

    val participantSessionManager = new ParticipantSessionManager
    val ledgerSession = new LedgerSession(config, participantSessionManager)
    val suites = suiteConstructors.map(constructor => constructor(ledgerSession))
    val testCount = suites.map(_.tests.size).sum

    logger.info(s"Running $testCount tests, ${math.min(testCount, concurrentTestRuns)} at a time.")

    Source(suites.flatMap(suite => suite.tests.map(suite -> _)))
      .mapAsync(concurrentTestRuns) {
        case (suite, test) => run(test, suite.session).map((suite, test, _))
      }
      .map((summarize _).tupled)
      .runWith(Sink.seq)
      .map(_.toVector)
      .recover { case NonFatal(e) => throw LedgerTestSuiteRunner.UncaughtExceptionError(e) }
      .onComplete { result =>
        participantSessionManager.closeAll()
        completionCallback(result)
        executionContext.shutdown()
      }
  }

  def verifyRequirementsAndRun(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {
    verifyRequirements.fold(
      throwable => completionCallback(Failure(throwable)),
      _ => run(completionCallback)
    )
  }
}
