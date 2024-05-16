// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.locks.StampedLock
import scala.concurrent.blocking

trait HasReadWriteLock {

  protected val lock = new StampedLock()

  def withReadLock[A, E, F[_]: Thereafter](
      fn: => EitherT[F, E, A]
  ): EitherT[F, E, A] = {
    val stamp = blocking(lock.readLock())
    fn.thereafter(_ => lock.unlockRead(stamp))
  }

  def withWriteLock[A, E, F[_]: Thereafter](
      fn: => EitherT[F, E, A]
  ): EitherT[F, E, A] = {
    val stamp = blocking(lock.writeLock())
    fn.thereafter(_ => lock.unlockWrite(stamp))
  }

}
