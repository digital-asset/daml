// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.daml.ledger.api.testtool.infrastructure.FutureAssertions.ExpectedFailureException
import org.slf4j.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.concurrent.duration._

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

  implicit val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  def succeedsAfter[V](delay: FiniteDuration)(test: => Future[V]): Future[V] = {
    scheduler.schedule(() => test, delay.toMillis, TimeUnit.MILLISECONDS).get()
  }

  def succeedsEventually(delay: FiniteDuration = 10.millis, maxInterval: FiniteDuration)(
      test: => Future[_]
  )(implicit ec: ExecutionContext): Future[Any] = {
    if ((maxInterval - delay) <= Duration.Zero) {
      succeedsAfter(delay)(test)
    } else {
      succeedsAfter(delay)(test).recoverWith { case NonFatal(_) =>
        succeedsEventually(delay, maxInterval - delay)(test)
      }
    }
  }

  def succeedsUntil(delay: FiniteDuration = 10.millis, succeedDuration: FiniteDuration)(
      test: => Future[_]
  )(implicit ec: ExecutionContext): Future[Any] = {
    if ((succeedDuration - delay) <= Duration.Zero) {
      succeedsAfter(delay)(test)
    } else {
      succeedsAfter(delay)(test).flatMap { _ =>
        succeedsEventually(delay, succeedDuration - delay)(test)
      }
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
  )(logger: Logger): Future[_] = {
    if (runs) {
      assertions
    } else {
      logger.warn(s"Not running optional assertions: $description")
      Future.unit
    }
  }
  final class ExpectedFailureException[T](context: String, value: T)
      extends NoSuchElementException(
        s"Expected a failure when $context, but got a successful result of: $value"
      )

}
final case class ParallelTestFailureException(message: String, failure: Throwable)
    extends RuntimeException(message, failure)
