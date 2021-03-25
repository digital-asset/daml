// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.{ExecutionException, TimeoutException}
import java.util.{Timer, TimerTask}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestCasesRunner._
import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration.ClosedWorldWaitingForAllParticipants
import com.daml.ledger.api.testtool.infrastructure.Result.Retired
import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSession,
  ParticipantTestContext,
}
import io.grpc.{Channel, ClientInterceptor}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

object LedgerTestCasesRunner {
  private val DefaultTimeout = 30.seconds

  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestCasesRunner])

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private final class UncaughtExceptionError(cause: Throwable)
      extends RuntimeException(uncaughtExceptionErrorMessage, cause)

}

final class LedgerTestCasesRunner(
    testCases: Vector[LedgerTestCase],
    participants: Vector[Channel],
    partyAllocation: PartyAllocationConfiguration = ClosedWorldWaitingForAllParticipants,
    shuffleParticipants: Boolean = false,
    timeoutScaleFactor: Double = 1.0,
    concurrentTestRuns: Int = 8,
    uploadDars: Boolean = true,
    identifierSuffix: String = "test",
    commandInterceptors: Seq[ClientInterceptor] = Seq.empty,
) {
  private[this] val verifyRequirements: Try[Unit] =
    Try {
      require(timeoutScaleFactor > 0, "The timeout scale factor must be strictly positive")
      require(identifierSuffix.nonEmpty, "The identifier suffix cannot be an empty string")
    }

  private def start(test: LedgerTestCase, session: LedgerSession)(implicit
      executionContext: ExecutionContext
  ): Future[Duration] = {
    def logAndStart(repetition: Int): Future[Duration] = {
      logger.info(s"Starting '${test.description}'. Run: $repetition out of ${test.repeated}")
      startSingle(test, session, repetition)
    }

    (2 to test.repeated).foldLeft(logAndStart(1)) { (result, repetition) =>
      result.flatMap(_ => logAndStart(repetition))
    }
  }

  private def startSingle(test: LedgerTestCase, session: LedgerSession, repetition: Int)(implicit
      executionContext: ExecutionContext
  ): Future[Duration] = {
    val execution = Promise[Duration]()
    val scaledTimeout = DefaultTimeout * timeoutScaleFactor * test.timeoutScale

    val startedTest =
      session
        .createTestContext(s"${test.shortIdentifier}_$repetition", identifierSuffix)
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

  private def result(
      startedTest: Future[Duration]
  )(implicit executionContext: ExecutionContext): Future[Either[Result.Failure, Result.Success]] =
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
    LedgerTestSummary(suite.name, test.name, test.description, result)

  private def run(test: LedgerTestCase, session: LedgerSession)(implicit
      executionContext: ExecutionContext
  ): Future[Either[Result.Failure, Result.Success]] =
    result(start(test, session))

  private def uploadDar(context: ParticipantTestContext, name: String)(implicit
      executionContext: ExecutionContext
  ): Future[Unit] = {
    logger.info(s"""Uploading DAR "$name"...""")
    context.uploadDarFile(Dars.read(name)).andThen { case _ =>
      logger.info(s"""Uploaded DAR "$name".""")
    }
  }

  private def uploadDarsIfRequired(
      sessions: Vector[ParticipantSession]
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    if (uploadDars) {
      Future
        .sequence(sessions.map { session =>
          for {
            context <- session.createInitContext("upload-dars", identifierSuffix)
            _ <- Future.sequence(Dars.resources.map(uploadDar(context, _)))
          } yield ()
        })
        .map(_ => ())
    } else {
      Future.successful(logger.info("DAR files upload skipped."))
    }

  private def createActorSystem(): ActorSystem =
    ActorSystem(classOf[LedgerTestCasesRunner].getSimpleName)

  private def runTestCases(
      ledgerSession: LedgerSession,
      testCases: Vector[LedgerTestCase],
      concurrency: Int,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val testCount = testCases.size
    logger.info(s"Running $testCount tests, ${math.min(testCount, concurrency)} at a time.")
    Source(testCases.zipWithIndex)
      .mapAsyncUnordered(concurrency) { case (test, index) =>
        run(test, ledgerSession).map(summarize(test.suite, test, _) -> index)
      }
      .runWith(Sink.seq)
      .map(_.toVector.sortBy(_._2).map(_._1))
  }

  private def run(
      participants: Vector[Channel]
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val (concurrentTestCases, sequentialTestCases) = testCases.partition(_.runConcurrently)
    ParticipantSession(partyAllocation, participants, commandInterceptors)
      .flatMap { sessions =>
        val ledgerSession = LedgerSession(sessions, shuffleParticipants)
        val testResults =
          for {
            _ <- uploadDarsIfRequired(sessions)
            concurrentTestResults <- runTestCases(
              ledgerSession,
              concurrentTestCases,
              concurrentTestRuns,
            )(materializer, materializer.executionContext)
            sequentialTestResults <- runTestCases(
              ledgerSession,
              sequentialTestCases,
              concurrency = 1,
            )(materializer, materializer.executionContext)
          } yield concurrentTestResults ++ sequentialTestResults

        testResults
          .recover { case NonFatal(e) =>
            throw new LedgerTestCasesRunner.UncaughtExceptionError(e)
          }
      }
  }

  private def prepareResourcesAndRun(implicit
      executionContext: ExecutionContext
  ): Future[Vector[LedgerTestSummary]] = {

    val materializerResources =
      ResourceOwner.forMaterializerDirectly(createActorSystem).acquire()

    // When running the tests, explicitly use the materializer's execution context
    // The tests will then be executed on it instead of the implicit one -- which
    // should only be used to manage resources' lifecycle
    val results =
      for {
        materializer <- materializerResources.asFuture
        results <- run(participants)(materializer, materializer.executionContext)
      } yield results

    results.onComplete(_ => materializerResources.release())

    results
  }

  def runTests(implicit executionContext: ExecutionContext): Future[Vector[LedgerTestSummary]] =
    verifyRequirements.fold(
      Future.failed,
      _ => prepareResourcesAndRun,
    )
}
