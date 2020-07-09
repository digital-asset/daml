// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.{ExecutionException, TimeoutException}
import java.util.{Timer, TimerTask}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestCasesRunner._
import com.daml.ledger.api.testtool.infrastructure.Result.Retired
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionManager
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object LedgerTestCasesRunner {
  private val DefaultTimeout = 30.seconds

  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestCasesRunner])

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private final case class UncaughtExceptionError(cause: Throwable)
      extends RuntimeException(uncaughtExceptionErrorMessage)
}

final class LedgerTestCasesRunner(
    config: LedgerSessionConfiguration,
    testCases: Vector[LedgerTestCase],
    identifierSuffix: String,
    suiteTimeoutScale: Double,
    concurrentTestRuns: Int,
) {
  private[this] val verifyRequirements: Try[Unit] =
    Try {
      require(suiteTimeoutScale > 0, "The timeout scale factor must be strictly positive")
      require(identifierSuffix.nonEmpty, "The identifier suffix cannot be an empty string")
    }

  private def start(test: LedgerTestCase, session: LedgerSession)(
      implicit ec: ExecutionContext,
  ): Future[Duration] = {
    val execution = Promise[Duration]
    val scaledTimeout = DefaultTimeout * suiteTimeoutScale * test.timeoutScale

    val startedTest =
      session
        .createTestContext(test.shortIdentifier, identifierSuffix)
        .flatMap { context =>
          val start = System.nanoTime()
          val result = test(context).map(_ => Duration.fromNanos(System.nanoTime() - start))
          logger.info(s"Started '${test.description}' with a timeout of $scaledTimeout.")
          result
        }

    val testTimeout = new TimerTask {
      override def run(): Unit = {
        val message = s"Timeout of $scaledTimeout for '${test.description}' hit."
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

  private def result(startedTest: Future[Duration])(
      implicit ec: ExecutionContext): Future[Either[Result.Failure, Result.Success]] =
    startedTest
      .map[Either[Result.Failure, Result.Success]](duration => Right(Result.Succeeded(duration)))
      .recover[Either[Result.Failure, Result.Success]] {
        case Retired =>
          Right(Retired)
        case _: TimeoutException =>
          Left(Result.TimedOut)
        case failure: AssertionError =>
          Left(Result.Failed(failure))
        case NonFatal(box: ExecutionException) =>
          box.getCause match {
            case failure: AssertionError =>
              Left(Result.Failed(failure))
            case NonFatal(exception) =>
              Left(Result.FailedUnexpectedly(exception))
          }
        case NonFatal(exception) =>
          Left(Result.FailedUnexpectedly(exception))
      }

  private def summarize(
      suite: LedgerTestSuite,
      test: LedgerTestCase,
      result: Either[Result.Failure, Result.Success],
  ): LedgerTestSummary =
    LedgerTestSummary(suite.name, test.name, test.description, config, result)

  private def run(test: LedgerTestCase, session: LedgerSession)(
      implicit ec: ExecutionContext,
  ): Future[Either[Result.Failure, Result.Success]] =
    result(start(test, session))

  private def run(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {

    val system: ActorSystem = ActorSystem(classOf[LedgerTestCasesRunner].getSimpleName)
    implicit val materializer: Materializer = Materializer(system)
    implicit val executionContext: ExecutionContext = materializer.executionContext

    val participantSessionManager = new ParticipantSessionManager
    val ledgerSession = new LedgerSession(config, participantSessionManager)

    def runTestCases(
        testCases: Vector[LedgerTestCase],
        concurrency: Int,
    ): Future[Vector[LedgerTestSummary]] = {
      val testCount = testCases.size
      logger.info(s"Running $testCount tests, ${math.min(testCount, concurrency)} at a time.")
      Source(testCases.zipWithIndex)
        .mapAsyncUnordered(concurrency) {
          case (test, index) =>
            run(test, ledgerSession).map(summarize(test.suite, test, _) -> index)
        }
        .runWith(Sink.seq)
        .map(_.toVector.sortBy(_._2).map(_._1))
    }

    val (concurrentTestCases, sequentialTestCases) = testCases.partition(_.runConcurrently)

    val testResults =
      for {
        concurrentTestsResults <- runTestCases(concurrentTestCases, concurrentTestRuns)
        sequentialTestsResults <- runTestCases(sequentialTestCases, concurrency = 1)
      } yield concurrentTestsResults ++ sequentialTestsResults

    testResults
      .recover { case NonFatal(e) => throw LedgerTestCasesRunner.UncaughtExceptionError(e) }
      .onComplete { result =>
        participantSessionManager.closeAll()
        materializer.shutdown()
        system.terminate().failed.foreach { throwable =>
          logger.error("The actor system failed to terminate.", throwable)
        }
        completionCallback(result)
      }
  }

  def verifyRequirementsAndRun(completionCallback: Try[Vector[LedgerTestSummary]] => Unit): Unit = {
    verifyRequirements.fold(
      throwable => completionCallback(Failure(throwable)),
      _ => run(completionCallback),
    )
  }
}
