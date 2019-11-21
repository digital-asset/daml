// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.util.concurrent.{Executors, TimeUnit}

import com.codahale.metrics.MetricRegistry
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/** A dedicated executor for blocking sql queries. */
final class SqlExecutor(noOfThread: Int, loggerFactory: NamedLoggerFactory, metrics: MetricRegistry)
    extends AutoCloseable {

  private[this] val logger = loggerFactory.getLogger(getClass)

  object Metrics {
    val waitAllTimer = metrics.timer("sql_all_wait")
    val execAllTimer = metrics.timer("sql_all_exec")
  }

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

  def runQuery[A](description: String, extraLog: Option[String])(block: => A): Future[A] = {
    val promise = Promise[A]
    val waitTimer = metrics.timer(s"sql_${description}_wait")
    val execTimer = metrics.timer(s"sql_${description}_exec")
    val startWait = System.nanoTime()
    executor.execute(() => {
      val waitNanos = System.nanoTime() - startWait
      extraLog.foreach(log =>
        logger.trace(s"$description: $log wait ${(waitNanos / 1E6).toLong} ms"))
      waitTimer.update(waitNanos, TimeUnit.NANOSECONDS)
      Metrics.waitAllTimer.update(waitNanos, TimeUnit.NANOSECONDS)
      val startExec = System.nanoTime()
      try {
        // Actual execution
        promise.success(block)
      } catch {
        case NonFatal(e) =>
          logger.error(
            s"$description: Got an exception while executing a SQL query. Rolled back the transaction.",
            e)
          promise.failure(e)
        case t: Throwable =>
          logger.error(s"$description: got a fatal error!", t) //fatal errors don't make it for some reason to the setUncaughtExceptionHandler above
          throw t
      }

      // decouple metrics updating from sql execution above
      try {
        val execNanos = System.nanoTime() - startExec
        extraLog.foreach(log =>
          logger.trace(s"$description: $log exec ${(execNanos / 1E6).toLong} ms"))
        execTimer.update(execNanos, TimeUnit.NANOSECONDS)
        Metrics.execAllTimer.update(execNanos, TimeUnit.NANOSECONDS)
      } catch {
        case t: Throwable =>
          logger.error("$description: Got an exception while updating timer metrics. Ignoring.", t)
      }
    })
    promise.future
  }

  override def close(): Unit = executor.shutdown()

}

object SqlExecutor {
  def apply(
      noOfThread: Int,
      loggerFactory: NamedLoggerFactory,
      metrics: MetricRegistry): SqlExecutor =
    new SqlExecutor(noOfThread, loggerFactory, metrics)
}
