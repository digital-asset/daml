// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.util.concurrent.Executors

import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/** A dedicated executor for blocking sql queries. */
final class SqlExecutor(noOfThread: Int, loggerFactory: NamedLoggerFactory, mm: MetricsManager)
    extends AutoCloseable {

  private[this] val logger = loggerFactory.getLogger(getClass)

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
    val waitTimer = mm.unmanagedTimer(s"sql_${description}_wait")
    val waitAllTimer = mm.unmanagedTimer("sql_all_wait")
    executor.execute(() => {
      waitTimer.time()
      waitAllTimer.time()
      try {
        val execTimer = mm.unmanagedTimer(s"sql_${description}_exec")
        val execAllTimer = mm.unmanagedTimer("sql_all_exec")
        val res = block()
        execTimer.time()
        execAllTimer.time()
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
  def apply(noOfThread: Int, loggerFactory: NamedLoggerFactory, mm: MetricsManager): SqlExecutor =
    new SqlExecutor(noOfThread, loggerFactory, mm)
}
