// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.util.concurrent.atomic.AtomicBoolean

import com.daml.ledger.participant.state.v2.ReadService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import org.slf4j.LoggerFactory

import scala.concurrent.Await

object StandaloneIndexerServer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(readService: ReadService, jdbcUrl: String): AutoCloseable = {
    val server = PostgresIndexer.create(readService, jdbcUrl)
    val indexHandleF = server.flatMap(
      _.subscribe(
        readService,
        t => logger.error("error while processing state updates", t),
        () => logger.info("successfully finished processing state updates")))(DEC)

    val indexFeedHandle = Await.result(indexHandleF, PostgresIndexer.asyncTolerance)
    logger.info("Started Indexer Server")

    val closed = new AtomicBoolean(false)

    new AutoCloseable {
      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          val _ = Await.result(indexFeedHandle.stop(), PostgresIndexer.asyncTolerance)
        }
      }
    }
  }
}
