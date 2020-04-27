// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.daml.resources.TestResourceOwner._

import scala.concurrent.{ExecutionContext, Future}

final class TestResourceOwner[T](acquire: Future[T], release: T => Future[Unit])
    extends ResourceOwner[T] {
  private val acquired = new AtomicBoolean(false)

  def hasBeenAcquired: Boolean = acquired.get

  def acquire()(implicit executionContext: ExecutionContext): Resource[T] = {
    if (!acquired.compareAndSet(false, true)) {
      throw new TriedToAcquireTwice
    }
    Resource(acquire)(
      value =>
        if (acquired.compareAndSet(true, false))
          release(value)
        else
          Future.failed(new TriedToReleaseTwice)
    )
  }
}

object TestResourceOwner {
  def apply[T](value: T): TestResourceOwner[T] =
    new TestResourceOwner(Future.successful(value), _ => Future.successful(()))

  final class TriedToAcquireTwice extends Exception("Tried to acquire twice.")

  final class TriedToReleaseTwice extends Exception("Tried to release twice.")
}
