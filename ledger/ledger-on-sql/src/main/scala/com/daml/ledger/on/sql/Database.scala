// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.on.sql.queries.Queries.InvalidDatabaseException
import com.daml.ledger.on.sql.queries.{H2Queries, Queries, SqliteQueries}

sealed trait Database {
  val queries: Queries

  val maximumPoolSize: Option[Int]
}

object Database {
  def apply(jdbcUrl: String): Database = {
    jdbcUrl match {
      case url if url.startsWith("jdbc:h2:") => new H2Database
      case url if url.startsWith("jdbc:sqlite:") => new SqliteDatabase
      case _ => throw new InvalidDatabaseException(jdbcUrl)
    }
  }

  final class H2Database extends Database {
    override val queries: Queries = new H2Queries

    override val maximumPoolSize: Option[Int] = None
  }

  final class SqliteDatabase extends Database {
    override val queries: Queries = new SqliteQueries

    override val maximumPoolSize: Option[Int] = Some(1)
  }
}
