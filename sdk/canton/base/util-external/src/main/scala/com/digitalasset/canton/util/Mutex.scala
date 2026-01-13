// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.util.concurrent.locks.ReentrantLock

/** Lock to be used instead of blocking synchronized
  *
  * The fork join pool is leaking threads potentially with every invocation of blocking. Therefore,
  * only invoke it if really necessary.
  */
class Mutex {
  private val lock = new ReentrantLock()

  @deprecated("use exclusive, not synchronize", since = "3.4")
  def synchronized[T](body: => T): T = sys.error("use exclusive to distinguish")

  def exclusive[T](f: => T): T = {
    def runAndUnlock() =
      try {
        f
      } finally {
        lock.unlock()
      }
    if (lock.tryLock()) {
      runAndUnlock()
    } else {
      // this may trigger a ForkJoinPool.managedBlock
      scala.concurrent.blocking {
        lock.lock()
        runAndUnlock()
      }
    }
  }
}

object Mutex {
  def apply(): Mutex = new Mutex
}
