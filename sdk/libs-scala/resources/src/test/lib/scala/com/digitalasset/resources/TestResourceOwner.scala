// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.daml.resources.TestResourceOwner._

import scala.concurrent.Future

final class TestResourceOwner[T](acquire: Future[T], release: T => Future[Unit])
    extends AbstractResourceOwner[TestContext, T] {
  private val acquired = new AtomicBoolean(false)

  def hasBeenAcquired: Boolean = acquired.get

  def acquire()(implicit context: TestContext): Resource[TestContext, T] = {
    if (!acquired.compareAndSet(false, true)) {
      throw new TriedToAcquireTwice
    }
    ReleasableResource(acquire)(value =>
      if (acquired.compareAndSet(true, false))
        release(value)
      else
        Future.failed(new TriedToReleaseTwice)
    )
  }
}

object TestResourceOwner {
  def apply[T](value: T): TestResourceOwner[T] =
    new TestResourceOwner(Future.successful(value), _ => Future.unit)

  final class TriedToAcquireTwice extends Exception("Tried to acquire twice.")

  final class TriedToReleaseTwice extends Exception("Tried to release twice.")
}
