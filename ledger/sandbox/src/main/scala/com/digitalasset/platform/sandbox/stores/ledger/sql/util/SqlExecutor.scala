// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.util.concurrent.Executors

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

  def runQuery[A](block: () => A): Future[A] = {
    val promise = Promise[A]
    executor.execute(() => {
      try {
        promise.success(block())
      } catch {
        case NonFatal(e) =>
          promise.failure(e)
      }
    })
    promise.future
  }

  override def close(): Unit = executor.shutdown()

}

object SqlExecutor {
  def apply(noOfThread: Int): SqlExecutor = new SqlExecutor(noOfThread)
}
