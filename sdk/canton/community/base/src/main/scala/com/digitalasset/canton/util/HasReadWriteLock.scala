// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.locks.StampedLock
import scala.concurrent.blocking

trait HasReadWriteLock {

  protected val lock = new StampedLock()

  def withReadLock[A, F[_]: Thereafter](
      fn: => F[A]
  ): F[A] = {
    val stamp = blocking(lock.readLock())
    fn.thereafter(_ => lock.unlockRead(stamp))
  }

  def withWriteLock[A, F[_]: Thereafter](
      fn: => F[A]
  ): F[A] = {
    val stamp = blocking(lock.writeLock())
    fn.thereafter(_ => lock.unlockWrite(stamp))
  }

}
