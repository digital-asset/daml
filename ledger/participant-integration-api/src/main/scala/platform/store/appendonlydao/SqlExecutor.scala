// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import com.daml.logging.LoggingContext
import com.daml.metrics.DatabaseMetrics

import java.sql.Connection
import scala.concurrent.Future

private[platform] trait SqlExecutor {
  def executeSql[T](databaseMetrics: DatabaseMetrics)(sql: Connection => T)(implicit
      loggingContext: LoggingContext
  ): Future[T]
}
