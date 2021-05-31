// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.FutureAssertions.ExpectedFailureException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class FutureAssertions[T](future: Future[T]) {

  /** Checks that the future failed, and returns the throwable.
    * We use this instead of `Future#failed` because the error message that delivers is unhelpful.
    * It doesn't tell us what the value actually was.
    */
  def mustFail(context: String)(implicit executionContext: ExecutionContext): Future[Throwable] =
    future.transform {
      case Failure(throwable) =>
        Success(throwable)
      case Success(value) =>
        Failure(new ExpectedFailureException(context, value))
    }
}

object FutureAssertions {

  final class ExpectedFailureException[T](context: String, value: T)
      extends NoSuchElementException(
        s"Expected a failure when $context, but got a successful result of: $value"
      )

}
