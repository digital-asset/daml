// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.locks.StampedLock
import scala.concurrent.{ExecutionContext, Future, blocking}

trait HasReadWriteLock {

  protected val lock = new StampedLock()

  def withReadLock[A, E](
      fn: => EitherT[Future, E, A]
  )(implicit ec: ExecutionContext): EitherT[Future, E, A] = {
    val stamp = blocking(lock.readLock())
    fn.thereafter(_ => lock.unlockRead(stamp))
  }

  def withWriteLock[A, E](
      fn: => EitherT[Future, E, A]
  )(implicit ec: ExecutionContext): EitherT[Future, E, A] = {
    val stamp = blocking(lock.writeLock())
    fn.thereafter(_ => lock.unlockWrite(stamp))
  }

}
