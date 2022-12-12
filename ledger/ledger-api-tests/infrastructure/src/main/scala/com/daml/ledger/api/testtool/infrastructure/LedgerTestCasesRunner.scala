// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.{ExecutionException, TimeoutException}
import java.util.{Timer, TimerTask}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestCasesRunner._
import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration.ClosedWorldWaitingForAllParticipants
import com.daml.ledger.api.testtool.infrastructure.future.FutureUtil
import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSession,
  ParticipantTestContext,
}
import io.grpc.ClientInterceptor
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
    participantChannels: Vector[ChannelEndpoint],
    maxConnectionAttempts: Int = 10,
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
      require(
        maxConnectionAttempts > 0,
        "The number of connection attempts must be strictly positive",
      )
      require(timeoutScaleFactor > 0, "The timeout scale factor must be strictly positive")
      require(identifierSuffix.nonEmpty, "The identifier suffix cannot be an empty string")
    }

  def runTests(implicit executionContext: ExecutionContext): Future[Vector[LedgerTestSummary]] =
    verifyRequirements.fold(
      Future.failed,
      _ => prepareResourcesAndRun,
    )

  private def createTestContextAndStart(
      test: LedgerTestCase.Repetition,
      session: LedgerSession,
  )(implicit executionContext: ExecutionContext): Future[Duration] = {
    val execution = Promise[Duration]()
    val scaledTimeout = DefaultTimeout * timeoutScaleFactor * test.timeoutScale

    val testName =
      test.repetition.fold[String](test.shortIdentifier)(r => s"${test.shortIdentifier}_${r._1}")
    val startedTest =
      session
        .createTestContext(testName, identifierSuffix)
        .flatMap { context =>
          val start = System.nanoTime()
          val result = test
            .allocatePartiesAndRun(context)
            .map(_ => Duration.fromNanos(System.nanoTime() - start))
          logger.info(
            s"Started '${test.description}'${test.repetition.fold("")(r => s" (${r._1}/${r._2})")} with a timeout of $scaledTimeout."
          )
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
        case Result.Retired =>
          Right(Result.Retired)
        case Result.Excluded(reason) =>
          Right(Result.Excluded(reason))
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

  private def run(
      test: LedgerTestCase.Repetition,
      session: LedgerSession,
  )(implicit executionContext: ExecutionContext): Future[Either[Result.Failure, Result.Success]] =
    result(createTestContextAndStart(test, session))

  private def uploadDarsIfRequired(
      sessions: Vector[ParticipantSession]
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    if (uploadDars) {
      FutureUtil
        .sequential(sessions) { session =>
          logger.info(s"Uploading DAR files for session $session")
          for {
            context <- session.createInitContext(
              applicationId = "upload-dars",
              identifierSuffix = identifierSuffix,
              features = session.features,
            )
            // upload the dars sequentially to avoid conflicts
            _ <- FutureUtil.sequential(Dars.resources)(uploadDar(context, _))
          } yield ()
        }
        .map(_ => ())
    } else {
      Future.successful(logger.info("DAR files upload skipped."))
    }

  private def uploadDar(
      context: ParticipantTestContext,
      name: String,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.info(s"""Uploading DAR "$name"...""")
    context
      .uploadDarFile(Dars.read(name))
      .map { _ =>
        logger.info(s"""Uploaded DAR "$name".""")
      }
      .recover { case NonFatal(exception) =>
        throw new Errors.DarUploadException(name, exception)
      }
  }

  private def createActorSystem: ActorSystem =
    ActorSystem(classOf[LedgerTestCasesRunner].getSimpleName)

  private def runTestCases(
      ledgerSession: LedgerSession,
      testCases: Vector[LedgerTestCase],
      concurrency: Int,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val testCaseRepetitions = testCases.flatMap(_.repetitions)
    val testCount = testCaseRepetitions.size
    logger.info(s"Running $testCount tests with concurrency of $concurrency.")
    Source(testCaseRepetitions.zipWithIndex)
      .mapAsyncUnordered(concurrency) { case (test, index) =>
        run(test, ledgerSession).map(summarize(test.suite, test.testCase, _) -> index)
      }
      .runWith(Sink.seq)
      .map(_.toVector.sortBy(_._2).map(_._1))
  }

  private def run(
      participantChannels: Vector[ChannelEndpoint]
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val sessions: Future[Vector[ParticipantSession]] = ParticipantSession.createSessions(
      partyAllocationConfig = partyAllocation,
      participantChannels = participantChannels,
      maxConnectionAttempts = maxConnectionAttempts,
      commandInterceptors = commandInterceptors,
      timeoutScaleFactor = timeoutScaleFactor,
    )
    sessions
      .flatMap { sessions: Vector[ParticipantSession] =>
        // All the participants should support the same features (for testing at least)
        val ledgerFeatures = sessions.head.features
        val (disabledTestCases, enabledTestCases) =
          testCases.partitionMap(testCase =>
            testCase
              .isEnabled(ledgerFeatures, sessions.size)
              .fold(disabledReason => Left(testCase -> disabledReason), _ => Right(testCase))
          )
        val excludedTestResults = disabledTestCases
          .map { case (testCase, disabledReason) =>
            LedgerTestSummary(
              testCase.suite.name,
              testCase.name,
              testCase.description,
              Right(Result.Excluded(disabledReason)),
            )
          }
        val (concurrentTestCases, sequentialTestCases) =
          enabledTestCases.partition(_.runConcurrently)
        val ledgerSession = LedgerSession(
          sessions,
          shuffleParticipants,
        )
        val testResults =
          for {
            _ <- uploadDarsIfRequired(sessions)
            concurrentTestResults <- runTestCases(
              ledgerSession,
              concurrentTestCases,
              concurrentTestRuns,
            )(materializer, executionContext)
            sequentialTestResults <- runTestCases(
              ledgerSession,
              sequentialTestCases,
              concurrency = 1,
            )(materializer, executionContext)
          } yield concurrentTestResults ++ sequentialTestResults ++ excludedTestResults

        testResults.recover {
          case NonFatal(e) if !e.isInstanceOf[Errors.FrameworkException] =>
            throw new LedgerTestCasesRunner.UncaughtExceptionError(e)
        }
      }
  }

  private def prepareResourcesAndRun(implicit
      executionContext: ExecutionContext
  ): Future[Vector[LedgerTestSummary]] = {

    val materializerResources =
      ResourceOwner.forMaterializerDirectly(() => createActorSystem).acquire()

    // When running the tests, explicitly use the materializer's execution context
    // The tests will then be executed on it instead of the implicit one -- which
    // should only be used to manage resources' lifecycle
    val results =
      for {
        materializer <- materializerResources.asFuture
        results <- run(participantChannels)(materializer, executionContext)
      } yield results

    results.onComplete(_ => materializerResources.release())

    results
  }

}
