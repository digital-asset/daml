// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.ledger.api.health.ReportsHealth

import java.sql.Connection

/** A helper to run JDBC queries using a pool of managed connections */
private[platform] trait JdbcConnectionProvider extends ReportsHealth {

  /** Blocks are running in a single transaction as the commit happens when the connection
    * is returned to the pool.
    * The block must not recursively call [[runSQL]], as this could result in a deadlock
    * waiting for a free connection from the same pool.
    */
  def runSQL[T](databaseMetrics: DatabaseMetrics)(block: Connection => T): T
}
