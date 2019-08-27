// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/** A dedicated executor for blocking sql queries. */
class SqlExecutor(noOfThread: Int) extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val executor =
    Executors.newFixedThreadPool(
      noOfThread,
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("sql-executor-%d")
        .setUncaughtExceptionHandler((_, e) => {
          logger.error("Got an uncaught exception in SQL executor!", e)
        })
        .build()
    )

  def runQuery[A](description: => String, block: () => A): Future[A] = {
    val promise = Promise[A]
    val startWait = System.nanoTime()
    executor.execute(() => {
      try {
        val elapsedWait = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startWait)
        val start = System.nanoTime()
        val res = block()
        val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)

        if (logger.isTraceEnabled) {
          logger.trace(
            s"""DB Operation "$description": wait time ${elapsedWait}ms, execution time ${elapsed}ms""")
        }

        promise.success(res)
      } catch {
        case NonFatal(e) =>
          promise.failure(e)
        case t: Throwable =>
          logger.error("got a fatal error!", t) //fatal errors don't make it for some reason to the setUncaughtExceptionHandler above
          throw t
      }
    })
    promise.future
  }

  override def close(): Unit = executor.shutdown()

}

object SqlExecutor {
  def apply(noOfThread: Int): SqlExecutor = new SqlExecutor(noOfThread)
}
