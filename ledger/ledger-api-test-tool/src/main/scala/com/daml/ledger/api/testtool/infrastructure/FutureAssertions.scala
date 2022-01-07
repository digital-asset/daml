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

  def assertAfter[V](
      delay: FiniteDuration
  )(test: => Future[V]): Future[V] = {
    Delayed.Future.by(delay)(test)
  }

  def succeedsEventually[V](
      delay: FiniteDuration = 100.millis,
      maxInterval: FiniteDuration,
      description: String,
  )(
      test: => Future[V]
  )(implicit ec: ExecutionContext, loggingContext: LoggingContext): Future[V] = {
    def internalSucceedsEventually(interval: FiniteDuration): Future[V] = {
      val newMaxInterval = interval - delay
      if (newMaxInterval < Duration.Zero) {
        test.andThen { case Failure(exception) =>
          logger.error(
            s"Assertion never succeeded after $maxInterval with a delay of $delay. Description: $description",
            exception,
          )
        }
      } else {
        if (newMaxInterval == Duration.Zero) {
          assertAfter(delay)(test)
        } else {
          assertAfter(delay)(test).recoverWith { case NonFatal(ex) =>
            logger.debug(
              s"Failed assertion: $description. Running again with new max interval $newMaxInterval",
              ex,
            )
            internalSucceedsEventually(newMaxInterval)
          }
        }
      }
    }

    internalSucceedsEventually(maxInterval)
  }

  def succeedsUntil[V](
      delay: FiniteDuration = 100.millis,
      succeedDuration: FiniteDuration,
      succeedDeadline: Option[Instant] = None,
  )(
      test: => Future[V]
  )(implicit ec: ExecutionContext, loggingContext: LoggingContext): Future[V] = {
    def internalSucceedsUntil(interval: FiniteDuration): Future[V] = {
      val newMaxInterval = interval - delay
      if (
        succeedDeadline.exists(
          _.isBefore(Instant.now().plusSeconds(delay.toSeconds))
        ) || newMaxInterval < Duration.Zero
      ) {
        test
      } else {
        if (newMaxInterval == Duration.Zero) {
          assertAfter(delay)(test)
        } else {
          assertAfter(delay)(test)
            .flatMap { _ =>
              internalSucceedsUntil(newMaxInterval)
            }
        }
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
  )(testCase: T => Future[Unit])(implicit ec: ExecutionContext): Future[Seq[Unit]] = {
    Future
      .traverse(data)(input =>
        testCase(input).map(Right(_)).recover { case NonFatal(ex) =>
          Left(input -> ex)
        }
      )
      .map { results =>
        val (failures, successes) = results.partitionMap(identity)
        if (failures.nonEmpty) {
          throw ParallelTestFailureException(
            s"Failed parallel test case. Failures: ${failures.length}. Success: ${successes.length}\nFailed inputs: ${failures
              .map(_._1)
              .mkString("[", ",", "]")}",
            failures.last._2,
          )
        } else {
          successes
        }
      }
  }

  def optionalAssertion(runs: Boolean, description: String)(
      assertions: => Future[_]
  )(implicit loggingContext: LoggingContext): Future[_] = if (runs) {
    assertions
  } else {
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
