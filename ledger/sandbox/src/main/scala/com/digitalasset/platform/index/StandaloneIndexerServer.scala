// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}

import scala.concurrent.duration._

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  def apply(readService: ReadService, jdbcUrl: String): AutoCloseable = {

    val indexer = RecoveringIndexer(10.seconds, JdbcIndexer.asyncTolerance)

    indexer.start(() =>
      PostgresIndexer.create(readService, jdbcUrl).flatMap(_.subscribe(readService))(DEC))

    indexer
  }
}
