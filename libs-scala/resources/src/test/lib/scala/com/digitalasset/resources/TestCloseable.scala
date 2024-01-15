// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.daml.resources.TestCloseable._

final class TestCloseable[T](val value: T, acquired: AtomicBoolean) extends AutoCloseable {
  if (!acquired.compareAndSet(false, true)) {
    throw new TriedToAcquireTwice
  }

  override def close(): Unit = {
    if (!acquired.compareAndSet(true, false)) {
      throw new TriedToReleaseTwice
    }
  }
}

object TestCloseable {
  final class TriedToAcquireTwice extends Exception("Tried to acquire twice.")

  final class TriedToReleaseTwice extends Exception("Tried to release twice.")
}
