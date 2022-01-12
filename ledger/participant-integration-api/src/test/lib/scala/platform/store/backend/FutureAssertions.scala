// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

// TODO pbatko copied from ledger-api-test-tool
final class FutureAssertions[T](future: Future[T]) {

  /** Checks that the future failed, and returns the throwable.
    * We use this instead of `Future#failed` because the error message that delivers is unhelpful.
    * It doesn't tell us what the value actually was.
    */
  def mustFail(context: String)(implicit executionContext: ExecutionContext): Future[Throwable] =
    handle(_ => true, context)

  def mustFail2[T2 <: Throwable](
      context: String
  )(implicit executionContext: ExecutionContext, tag: ClassTag[T2]): Future[T2] =
    handle2(_ => true, context)

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
      case Success(value) =>
        Failure(
          new AssertionError(
            s"Expected a failure when $context, but got a successful result of: $value"
          )
        )
      case Failure(other) => Failure(other)
    }

  private def handle2[T2 <: Throwable](predicate: Throwable => Boolean, context: String)(implicit
      executionContext: ExecutionContext,
      tag: ClassTag[T2],
  ): Future[T2] =
    future.transform {
      case Failure(throwable) if !tag.runtimeClass.isAssignableFrom(throwable.getClass) =>
        Failure(
          new AssertionError(
            s"Expected a failure of ${tag.runtimeClass} when $context, but got a failure of ${throwable.getClass}"
          )
        )
      case Failure(throwable) if predicate(throwable) => Success(throwable.asInstanceOf[T2])
      case Success(value) =>
        Failure(
          new AssertionError(
            s"Expected a failure when $context, but got a successful result of: $value"
          )
        )
      case Failure(other) => Failure(other)
    }

}
