// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import scala.collection.concurrent.TrieMap
import java.util.concurrent.Semaphore
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/** Utility for sharing a set of locks across threads. Each lock is identified by a key.
  *
  * Assumes that in practice there is a bound on the number of keys used, which can fit in memory.
  */
class LockSet[K]()(implicit ord: Ordering[K]) {
  val locks = new TrieMap[K, Semaphore]

  def withLocksOn[A](
      keys: Iterable[K]
  )(block: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    val acquired = new ListBuffer[Semaphore]()
    Future {
      keys.toSet.toSeq.sorted.foreach { key =>
        val lock = locks.getOrElseUpdate(key, new Semaphore(1))
        lock.acquire()
        acquired += lock
      }
    }
      .flatMap { _ => block }
      .transform { result =>
        acquired.reverse.foreach(_.release())
        result
      }
  }
}
