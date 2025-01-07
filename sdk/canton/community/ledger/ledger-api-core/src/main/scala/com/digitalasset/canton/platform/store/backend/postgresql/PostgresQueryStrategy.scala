// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy

import java.sql.Connection

object PostgresQueryStrategy extends QueryStrategy {

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"$elementColumnName = any($arrayColumnName)"

  override def anyOf(longs: Iterable[Long]): CompositeSql = {
    val longArray: Array[java.lang.Long] =
      longs.view.map(Long.box).toArray
    cSQL"= ANY($longArray::bigint[])"
  }

  override def anyOfStrings(strings: Iterable[String]): CompositeSql = {
    val stringArray: Array[String] =
      strings.toArray
    cSQL"= ANY($stringArray::text[])"
  }

  override def analyzeTable(tableName: String): CompositeSql =
    cSQL"ANALYZE #$tableName"

  override def forceSynchronousCommitForCurrentTransactionForPostgreSQL(
      connection: Connection
  ): Unit = SQL"SET LOCAL synchronous_commit TO ON".execute()(connection).discard
}
