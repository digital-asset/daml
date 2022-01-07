// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Instant

import com.daml.ledger.api.testtool.infrastructure.FutureAssertions.ExpectedFailureException
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.timer.Delayed

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

final class FutureAssertions[T](future: Future[T]) {

  /** Checks that the future failed, and returns the throwable.
    * We use this instead of `Future#failed` because the error message that delivers is unhelpful.
    * It doesn't tell us what the value actually was.
    */
  def mustFail(context: String)(implicit executionContext: ExecutionContext): Future[Throwable] =
    handle(_ => true, context)

  /** Checks that the future failed satisfying the predicate and returns the throwable.
    * We use this instead of `Future#failed` because the error message that delivers is unhelpful.
    * It doesn't tell us what the value actually was.
    */
  def mustFailWith(context: String)(
      predicate: Throwable => Boolean
  )(implicit executionContext: ExecutionContext): Future[Throwable] =
    handle(predicate, context)

  private def handle(predicate: Throwable => Boolean, context: String)(implicit
      executionContext: ExecutionContext
  ): Future[Throwable] =
    future.transform {
      case Failure(throwable) if predicate(throwable) => Success(throwable)
      case Success(value) => Failure(new ExpectedFailureException(context, value))
      case Failure(other) => Failure(other)
    }

}

object FutureAssertions {

  private val logger = ContextualizedLogger.get(getClass)

  /** Runs the test case after the specified delay
    */
  def assertAfter[V](
      delay: FiniteDuration
  )(test: => Future[V]): Future[V] =
    Delayed.Future.by(delay)(test)

  /** Run the test every [[retryDelay]] up to [[maxRetryDuration]].
    * The assertion will succeed if any of the test case runs are successful.
    * The assertion will fail if no test case runs are successful and the [[maxRetryDuration]] is exceeded.
    * The test case will run up to [[ceil(maxRetryDuration / retryDelay)]] times
    */
  def succeedsEventually[V](
      retryDelay: FiniteDuration = 100.millis,
      maxRetryDuration: FiniteDuration,
      description: String,
  )(
      test: => Future[V]
  )(implicit ec: ExecutionContext, loggingContext: LoggingContext): Future[V] = {
    def internalSucceedsEventually(remainingDuration: FiniteDuration): Future[V] = {
      val nextRetryRemainingDuration = remainingDuration - retryDelay
      if (nextRetryRemainingDuration < Duration.Zero) test.andThen { case Failure(exception) =>
        logger.error(
          s"Assertion never succeeded after $maxRetryDuration with a delay of $retryDelay. Description: $description",
          exception,
        )
      }
      else
        assertAfter(retryDelay)(test).recoverWith { case NonFatal(ex) =>
          logger.debug(
            s"Failed assertion: $description. Running again with new max duration $nextRetryRemainingDuration",
            ex,
          )
          internalSucceedsEventually(nextRetryRemainingDuration)
        }
    }

    internalSucceedsEventually(maxRetryDuration)
  }

  /** Run the test every [[rerunDelay]] for a duration of [[succeedDuration]] or until the current time exceeds [[succeedDeadline]].
    * The assertion will succeed if all of the test case runs are successful and [[succeedDuration]] is exceeded or [[succeedDeadline]] is exceeded.
    * The assertion will fail if any test case runs fail.
    */
  def succeedsUntil[V](
      rerunDelay: FiniteDuration = 100.millis,
      succeedDuration: FiniteDuration,
      succeedDeadline: Option[Instant] = None,
  )(
      test: => Future[V]
  )(implicit ec: ExecutionContext, loggingContext: LoggingContext): Future[V] = {
    def internalSucceedsUntil(remainingDuration: FiniteDuration): Future[V] = {
      val nextRerunRemainingDuration = remainingDuration - rerunDelay
      if (
        succeedDeadline.exists(
          _.isBefore(Instant.now().plusSeconds(rerunDelay.toSeconds))
        ) || nextRerunRemainingDuration < Duration.Zero
      ) test
      else
        assertAfter(rerunDelay)(test)
          .flatMap { _ =>
            internalSucceedsUntil(nextRerunRemainingDuration)
          }
    }

    internalSucceedsUntil(succeedDuration)
      .andThen { case Failure(exception) =>
        logger.error(
          s"Repeated assertion failed with a succeed duration of $succeedDuration.",
          exception,
        )
      }
  }

  def forAllParallel[T](
      data: Seq[T]
  )(testCase: T => Future[Unit])(implicit ec: ExecutionContext): Future[Seq[Unit]] = Future
    .traverse(data)(input =>
      testCase(input).map(Right(_)).recover { case NonFatal(ex) =>
        Left(input -> ex)
      }
    )
    .map { results =>
      val (failures, successes) = results.partitionMap(identity)
      if (failures.nonEmpty)
        throw ParallelTestFailureException(
          s"Failed parallel test case. Failures: ${failures.length}. Success: ${successes.length}\nFailed inputs: ${failures
            .map(_._1)
            .mkString("[", ",", "]")}",
          failures.last._2,
        )
      else successes
    }

  def optionalAssertion(runs: Boolean, description: String)(
      assertions: => Future[_]
  )(implicit loggingContext: LoggingContext): Future[_] = if (runs) assertions
  else {
    logger.warn(s"Not running optional assertions: $description")
    Future.unit
  }

  final class ExpectedFailureException[T](context: String, value: T)
      extends NoSuchElementException(
        s"Expected a failure when $context, but got a successful result of: $value"
      )

}

final case class ParallelTestFailureException(message: String, failure: Throwable)
    extends RuntimeException(message, failure)
