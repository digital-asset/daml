// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

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
import com.daml.ledger.api.tls.TlsConfiguration
import io.grpc.ClientInterceptor
import org.slf4j.LoggerFactory

import java.util.concurrent.{ExecutionException, TimeoutException}
import java.util.{Timer, TimerTask}
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
    participants: Vector[ChannelEndpoint],
    maxConnectionAttempts: Int = 10,
    partyAllocation: PartyAllocationConfiguration = ClosedWorldWaitingForAllParticipants,
    shuffleParticipants: Boolean = false,
    timeoutScaleFactor: Double = 1.0,
    concurrentTestRuns: Int = 8,
    uploadDars: Boolean = true,
    identifierSuffix: String = "test",
    commandInterceptors: Seq[ClientInterceptor] = Seq.empty,
    clientTlsConfiguration: Option[TlsConfiguration],
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

  private def start(
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
          val result = test(context).map(_ => Duration.fromNanos(System.nanoTime() - start))
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

  private def run(
      test: LedgerTestCase.Repetition,
      session: LedgerSession,
  )(implicit executionContext: ExecutionContext): Future[Either[Result.Failure, Result.Success]] =
    result(start(test, session))

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

  private def uploadDarsIfRequired(
      sessions: Vector[ParticipantSession],
      clientTlsConfiguration: Option[TlsConfiguration],
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    if (uploadDars) {
      Future
        .sequence(sessions.map { session =>
          for {
            context <- session.createInitContext(
              applicationId = "upload-dars",
              identifierSuffix = identifierSuffix,
              clientTlsConfiguration = clientTlsConfiguration,
            )
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
    val testCaseRepetitions = testCases.flatMap(_.repetitions)
    val testCount = testCaseRepetitions.size
    logger.info(s"Running $testCount tests, ${math.min(testCount, concurrency)} at a time.")
    Source(testCaseRepetitions.zipWithIndex)
      .mapAsyncUnordered(concurrency) { case (test, index) =>
        run(test, ledgerSession).map(summarize(test.suite, test.testCase, _) -> index)
      }
      .runWith(Sink.seq)
      .map(_.toVector.sortBy(_._2).map(_._1))
  }

  private def run(
      participants: Vector[ChannelEndpoint]
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val (concurrentTestCases, sequentialTestCases) = testCases.partition(_.runConcurrently)
    ParticipantSession(partyAllocation, participants, maxConnectionAttempts, commandInterceptors)
      .flatMap { sessions: Vector[ParticipantSession] =>
        val ledgerSession = LedgerSession(
          sessions,
          shuffleParticipants,
          clientTlsConfiguration = clientTlsConfiguration,
        )
        val testResults =
          for {
            _ <- uploadDarsIfRequired(sessions, clientTlsConfiguration)
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
