// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Id
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps

import java.util.concurrent.locks.StampedLock
import scala.concurrent.blocking
import scala.util.control.NonFatal

/** A stamped lock that allows passing around a lock handle to better guard methods that should only
  * be called with an active lock.
  *
  * For example:
  *
  * {{{
  * object Foo {
  *   val lock = new StampedLockWithHandle()
  *
  *   def bar() = lockWithWriteLockHandle { implicit writeLock =>
  *     // do something
  *     baz()
  *     // do more stuff
  *   }
  *
  *   def baz()(implicit writeLockHandle: lock.WriteLockHandle) = {
  *     // do some more stuff
  *   }
  * }
  * }}}
  *
  * In the above example, `baz` cannot be called unless a write lock was acquired specifically only
  * with `lock`.
  */
class StampedLockWithHandle {
  class ReadLockHandle private[StampedLockWithHandle] (
      private[StampedLockWithHandle] val stamp: Long
  )
  class WriteLockHandle private[StampedLockWithHandle] (
      private[StampedLockWithHandle] val stamp: Long
  )

  val lock = new StampedLock()

  /** Acquire a read lock, blocking if it's currently not possible.
    * @return
    *   A read lock handle with which the lock can be unlocked again.
    */
  def readLock(): ReadLockHandle = new ReadLockHandle(lock.readLock())

  /** Acquire a write lock, blocking if it's currently not possible.
    * @return
    *   A write lock handle with which the lock can be unlocked again.
    */
  def writeLock(): WriteLockHandle = new WriteLockHandle(lock.writeLock())

  /** Unlock a previously acquired read lock.
    */
  def unlockRead(readLockHandle: ReadLockHandle): Unit = lock.unlockRead(readLockHandle.stamp)

  /** Unlock a previously acquired write lock.
    */
  def unlockWrite(writeLockHandle: WriteLockHandle): Unit = lock.unlockWrite(writeLockHandle.stamp)

  /** Run a block of code within a read lock, providing the acquired read lock handle.
    */
  def withReadLockHandle[A, F[_]: Thereafter](fn: ReadLockHandle => F[A]): F[A] = {
    val stamp = blocking(readLock())
    try {
      fn(stamp).thereafter(_ => unlockRead(stamp))
    } catch {
      // Why not use finally here?
      // Because the sequence of events is as follows:
      // 1. acquire lock
      // 2. construct future from the by-name parameter
      // 3a. if 2. fails, catch NonFatal exceptions and unlock
      // 3b. if 2. succeeds, return the control and unlock only after the future completed
      // if we were to unlock in a finally clause, we would unlock while the future is still running
      case NonFatal(t) =>
        unlockRead(stamp)
        throw t
    }
  }

  /** Run a block of code within a read lock.
    */
  def withReadLock[A, F[_]: Thereafter](fn: => F[A]): F[A] =
    withReadLockHandle(_ => fn)

  /** Run a block of code within a read lock.
    */
  def withReadLock[A](fn: => A): A =
    withReadLockHandle(_ => Id(fn))

  /** Run a block of code within a write lock, providing the acquired write lock handle.
    */
  def withWriteLockHandle[A, F[_]: Thereafter](fn: WriteLockHandle => F[A]): F[A] = {
    val stamp = blocking(writeLock())
    try {
      fn(stamp).thereafter(_ => unlockWrite(stamp))
    } catch {
      // Why not use finally here?
      // Because the sequence of events is as follows:
      // 1. acquire lock
      // 2. construct future from the by-name parameter
      // 3a. if 2. fails, catch NonFatal exceptions and unlock
      // 3b. if 2. succeeds, return the control and unlock only after the future completed
      // if we were to unlock in a finally clause, we would unlock while the future is still running
      case NonFatal(t) =>
        unlockWrite(stamp)
        throw t
    }
  }

  /** Run a block of code within a write lock, providing the acquired write lock handle.
    */
  def withWriteLock[A, F[_]: Thereafter](fn: => F[A]): F[A] =
    withWriteLockHandle(_ => fn)

}
