// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util
import scala.collection.concurrent.TrieMap
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ListBuffer

/**
 * Utility for sharing a set of locks to be shared across threads. Each lock is identified by a key.
 *
 * Assumes that in practice there is a bound on the number of keys used, which can fit in memory.
 */
class LockSet[K]()(implicit ord: Ordering[K]) {
  val locks = new TrieMap[K, ReentrantLock]

  def withLocksOn[A](keys: Iterable[K])(block: => A): A = {
    val acquired = new ListBuffer[ReentrantLock]()
    try {
      keys.toSet.toSeq.sorted.foreach { key => 
        val lock = locks.getOrElseUpdate(key, new ReentrantLock())
        lock.lock() 
        acquired += lock
      }
      block
    } finally {
      acquired.reverse.foreach(_.unlock())
    }
  }
}
