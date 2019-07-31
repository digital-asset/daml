// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.util.concurrent.atomic.AtomicBoolean

import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import org.slf4j.LoggerFactory

import scala.concurrent.Await

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(readService: ReadService, jdbcUrl: String): AutoCloseable = {
    val server = JdbcIndexer.create(readService, jdbcUrl)
    val indexHandleF = server.flatMap(
      _.subscribe(
        readService,
        t => logger.error("error while processing state updates", t),
        () => logger.info("successfully finished processing state updates")))(DEC)

    val indexFeedHandle = Await.result(indexHandleF, JdbcIndexer.asyncTolerance)
    logger.info("Started Indexer Server")

    val closed = new AtomicBoolean(false)

    new AutoCloseable {
      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          val _ = Await.result(indexFeedHandle.stop(), JdbcIndexer.asyncTolerance)
        }
      }
    }
  }
}
