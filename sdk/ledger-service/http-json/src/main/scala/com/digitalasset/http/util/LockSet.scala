// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import java.util.concurrent.{Executors, Semaphore, TimeoutException, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.concurrent.duration.{Deadline, FiniteDuration, NANOSECONDS}

/** Utility for sharing a set of locks across threads. Each lock is identified by a key.
  *
  * Assumes that in practice there is a bound on the number of keys used, which can fit in memory.
  */
class LockSet[K](logger: ContextualizedLogger)(implicit ord: Ordering[K]) {
  private val locks = new TrieMap[K, Semaphore]

  private val acquisitionEc: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def withLocksOn[A](
      keysItr: Iterable[K],
      acquireTimeout: FiniteDuration,
      opName: String = "",
  )(block: => Future[A])(implicit callerEc: ExecutionContext, lc: LoggingContext): Future[A] = {
    import LockSet.{formatDeadline, fromNow}

    val keys = keysItr.toSet.toSeq.sorted
    val acquired = new ListBuffer[K]()

    // Use our own EC for this, to avoid thread starvation on the callers EC.
    val acquireAllLocks = Future {
      val deadline = fromNow(acquireTimeout) // Timeout starts ticking when the future is scheduled.
      logger.debug(
        s"$opName: Attempting to acquire locks for keys: $keys, by ${formatDeadline(deadline)}"
      )
      keys.foreach { key => if (acquireLock(opName, key, deadline)) acquired += key }
      logger.debug(s"$opName: Acquired locks for keys: $acquired")
      acquired.size == keys.size
    }(acquisitionEc)

    acquireAllLocks
      .flatMap { success =>
        if (success) block
        else Future.failed(new TimeoutException(s"Acquiring locks on $keys within $acquireTimeout"))
      }
      .transform { result =>
        acquired.reverse.foreach(releaseLock(opName, _))
        logger.debug(s"$opName: Released locks for keys: $acquired")
        result
      }(callerEc)
  }

  private def acquireLock(opName: String, key: K, deadline: Deadline)(implicit
      lc: LoggingContext
  ): Boolean =
    if (deadline.isOverdue()) {
      logger.debug(s"$opName: Not attempting to acquire lock for $key, as deadline has passed")
      false
    } else {
      val lock = locks.getOrElseUpdate(key, new Semaphore(1))
      logger.debug(s"$opName: Attempting to acquire lock for $key")
      val acquired = blocking { lock.tryAcquire(deadline.timeLeft.toMillis, TimeUnit.MILLISECONDS) }
      if (acquired) logger.debug(s"$opName: Acquired lock for $key")
      else logger.debug(s"$opName: Failed to acquire lock for $key before deadline")
      acquired
    }

  private def releaseLock(opName: String, key: K)(implicit lc: LoggingContext): Unit = {
    locks(key).release() // Lock for this key was added by acquireLock
    logger.debug(s"$opName: Released lock for $key")
  }
}

object LockSet {
  private val MaxDeadline = Deadline(Long.MaxValue, NANOSECONDS)

  // Safe from Inf/max values
  private def fromNow(timeout: FiniteDuration): Deadline =
    if (timeout >= MaxDeadline.timeLeft) MaxDeadline else timeout.fromNow

  private def formatDeadline(deadline: Deadline): String = {
    import java.time.{Instant, Duration => JavaDuration}
    import java.time.temporal.ChronoUnit
    Instant
      .now()
      .plus(JavaDuration.ofNanos(deadline.timeLeft.toNanos))
      .truncatedTo(ChronoUnit.MILLIS) // Nanos would be over-precise.
      .toString()
  }
}
