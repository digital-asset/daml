// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object DelayedReleaseResourceOwner {
  def apply[T](value: T, releaseDelay: FiniteDuration): TestResourceOwner[T] =
    new TestResourceOwner(
      Future.successful(value),
      _ => Future(Thread.sleep(releaseDelay.toMillis))(ExecutionContext.global),
    )
}
