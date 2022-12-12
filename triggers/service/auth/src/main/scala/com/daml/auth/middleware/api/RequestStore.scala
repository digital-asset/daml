// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.api

import scala.collection.mutable.LinkedHashMap
import scala.concurrent.duration.FiniteDuration

/** A key-value store with a maximum capacity and maximum storage duration.
  * @param maxCapacity Maximum number of requests that can be stored.
  * @param timeout Duration after which requests will be evicted.
  * @param monotonicClock Determines the current timestamp. The underlying clock must be monotonic.
  *   The JVM will use a monotonic clock for [[System.nanoTime]], if available, according to
  *   [[https://bugs.openjdk.java.net/browse/JDK-6458294?focusedCommentId=13823604&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-13823604 JDK bug 6458294]]
  * @tparam K The key type
  * @tparam V The value type
  */
private[middleware] class RequestStore[K, V](
    maxCapacity: Int,
    timeout: FiniteDuration,
    monotonicClock: () => Long = () => System.nanoTime,
) {

  /** Mapping from key to insertion timestamp and value.
    * The timestamp of later inserted elements must be greater or equal to the timestamp of earlier inserted elements.
    */
  private val store: LinkedHashMap[K, (Long, V)] = LinkedHashMap.empty

  /** Check whether the given [[timestamp]] timed out relative to the current time [[now]].
    */
  private def timedOut(now: Long, timestamp: Long): Boolean = {
    now - timestamp >= timeout.toNanos
  }

  private def evictTimedOut(now: Long): Unit = {
    // Remove items until their timestamp is more recent than the configured timeout.
    store.iterator
      .takeWhile { case (_, (t, _)) =>
        timedOut(now, t)
      }
      .foreach { case (k, _) =>
        store.remove(k)
      }
  }

  /** Insert a new key-value pair unless the maximum capacity is reached.
    * Evicts timed out elements before attempting insertion.
    * @return whether the key-value pair was inserted.
    */
  def put(key: K, value: => V): Boolean = {
    synchronized {
      val now = monotonicClock()
      evictTimedOut(now)
      if (store.size >= maxCapacity) {
        false
      } else {
        store.update(key, (now, value))
        true
      }
    }
  }

  /** Remove and return the value under the given key, if present and not timed out.
    */
  def pop(key: K): Option[V] = {
    synchronized {
      store.remove(key).flatMap {
        case (t, _) if timedOut(monotonicClock(), t) => None
        case (_, v) => Some(v)
      }
    }
  }
}
