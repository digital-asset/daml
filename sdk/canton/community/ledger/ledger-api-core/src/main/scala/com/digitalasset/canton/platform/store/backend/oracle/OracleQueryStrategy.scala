// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy

object OracleQueryStrategy extends QueryStrategy {

  override def columnEqualityBoolean(column: String, value: String): String =
    s"""case when ($column = $value) then 1 else 0 end"""

  override def booleanOrAggregationFunction: String = "max"

  override def lastByProxyAggregateFuction(
      singletonColumn: String,
      orderingColumn: String,
  ): String =
    s"max($singletonColumn) KEEP (DENSE_RANK FIRST ORDER BY $orderingColumn DESC)"

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"EXISTS (SELECT 1 FROM JSON_TABLE($arrayColumnName, '$$[*]' columns (value NUMBER PATH '$$')) WHERE value = $elementColumnName)"

  override def isTrue(booleanColumnName: String): String = s"$booleanColumnName = 1"

  override def constBooleanSelect(value: Boolean): String = if (value) "1" else "0"

  override def constBooleanWhere(value: Boolean): String = if (value) "1=1" else "0=1"

  // TODO(i13593): WARNING! this construction will lead to "= ANY(?, ?, ?, ..... ?)" SQLs, for which oracle has an upper limit of 1000
  override def anyOf(longs: Iterable[Long]): CompositeSql = {
    val longVector: Vector[java.lang.Long] =
      longs.view.map(Long.box).toVector
    cSQL"= ANY($longVector)"
  }

  // TODO(i13593): WARNING! this construction will lead to "= ANY(?, ?, ?, ..... ?)" SQLs, for which oracle has an upper limit of 1000
  override def anyOfStrings(strings: Iterable[String]): CompositeSql = {
    val stringVector: Vector[String] =
      strings.toVector
    cSQL"= ANY($stringVector)"
  }

  override def analyzeTable(tableName: String): CompositeSql =
    throw new UnsupportedOperationException("not supported for Oracle at the moment")
}
