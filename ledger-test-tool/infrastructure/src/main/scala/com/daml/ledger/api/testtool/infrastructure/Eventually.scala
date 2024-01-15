// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.timer.RetryStrategy
import com.daml.timer.RetryStrategy.{TooManyAttemptsException, UnhandledFailureException}

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}

object Eventually {

  /*
  Runs provided closure with the exponential back-off retry strategy for a number of `attempts`.
   */
  def eventually[A](assertionName: String, attempts: Int = 10, firstWaitTime: Duration = 10.millis)(
      runAssertion: => Future[A]
  )(implicit ec: ExecutionContext): Future[A] =
    RetryStrategy
      .exponentialBackoff(attempts, firstWaitTime) { (_, _) =>
        runAssertion
      }
      .recoverWith {
        case tooManyAttempts: TooManyAttemptsException =>
          Future.failed(
            tooManyAttempts.copy(message = s"$assertionName: ${tooManyAttempts.message}")
          )
        case unhandledFailure: UnhandledFailureException =>
          Future.failed(
            unhandledFailure.copy(message = s"$assertionName: ${unhandledFailure.message}")
          )
      }
}
