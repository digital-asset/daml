// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

object Eventually {
  private val withRetryStrategy = RetryStrategy.exponentialBackoff(10, 10.millis)

  def eventually[A](runAssertion: => Future[A])(implicit ec: ExecutionContext): Future[A] =
    withRetryStrategy { _ =>
      runAssertion
    }
}
