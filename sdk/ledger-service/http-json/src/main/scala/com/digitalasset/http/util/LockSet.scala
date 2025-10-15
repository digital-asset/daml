// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import scala.collection.concurrent.TrieMap
import java.util.concurrent.{Executors, Semaphore}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.{ContextualizedLogger, LoggingContext}

/** Utility for sharing a set of locks across threads. Each lock is identified by a key.
  *
  * Assumes that in practice there is a bound on the number of keys used, which can fit in memory.
  */
class LockSet[K](logger: ContextualizedLogger)(implicit ord: Ordering[K]) {
  val locks = new TrieMap[K, Semaphore]

  private val acquisitionEc: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def withLocksOn[A](
      keysItr: Iterable[K]
  )(block: => Future[A])(implicit callerEc: ExecutionContext, lc: LoggingContext): Future[A] = {
    val keys = keysItr.toSet.toSeq.sorted
    val acquired = new ListBuffer[K]()

    // Use our own EC for this, to avoid thread starvation on the callers EC.
    val acquireAllLocks = Future {
      logger.debug(s"Attempting to acquire locks for keys: $keys")
      keys.foreach { key =>
        acquireLock(key)
        acquired += key
      }
      logger.debug(s"Acquired locks for keys: $keys")
    }(acquisitionEc)

    acquireAllLocks
      .flatMap { _ => block }
      .transform { result =>
        acquired.reverse.foreach(releaseLock(_))
        logger.debug(s"Released locks for keys: $keys")
        result
      }(callerEc)
  }

  private def acquireLock(key: K)(implicit lc: LoggingContext): Unit = {
    val lock = locks.getOrElseUpdate(key, new Semaphore(1))
    logger.debug(s"Attempting to acquire lock for $key")
    lock.acquire()
    logger.debug(s"Acquired lock for $key")
  }

  private def releaseLock(key: K)(implicit lc: LoggingContext): Unit = {
    locks(key).release() // Lock for this key was added by acquireLock
    logger.debug(s"Released lock for $key")
  }
}
