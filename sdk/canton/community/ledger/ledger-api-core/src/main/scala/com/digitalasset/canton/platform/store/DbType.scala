// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

private[platform] sealed abstract class DbType(
    val name: String,
    val driver: String,
    val supportsParallelWrites: Boolean,
    val supportsAsynchronousCommits: Boolean,
)

object DbType {
  object Postgres
      extends DbType(
        "postgres",
        "org.postgresql.Driver",
        supportsParallelWrites = true,
        supportsAsynchronousCommits = true,
      )

  // H2 does not support concurrent, conditional updates to the ledger_end at read committed isolation
  // level: "It is possible that a transaction from one connection overtakes a transaction from a different
  // connection. Depending on the operations, this might result in different results, for example when conditionally
  // incrementing a value in a row." - from http://www.h2database.com/html/advanced.html
  object H2Database
      extends DbType(
        "h2",
        "org.h2.Driver",
        supportsParallelWrites = false,
        supportsAsynchronousCommits = false,
      )

  def jdbcType(jdbcUrl: String): DbType = jdbcUrl match {
    case h2 if h2.startsWith("jdbc:h2:") => H2Database
    case pg if pg.startsWith("jdbc:postgresql:") => Postgres
    case _ =>
      sys.error(s"JDBC URL doesn't match any supported databases (h2, pg)")
  }
}
