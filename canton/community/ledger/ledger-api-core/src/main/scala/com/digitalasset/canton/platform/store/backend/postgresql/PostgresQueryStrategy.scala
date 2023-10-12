// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy

object PostgresQueryStrategy extends QueryStrategy {

  override def arrayIntersectionNonEmptyClause(
      columnName: String,
      internedParties: Set[Int],
  ): CompositeSql = {
    require(internedParties.nonEmpty, "internedParties must be non-empty")
    // anorm does not like primitive arrays, so we need to box it
    val partiesArray: Array[java.lang.Integer] = internedParties.map(Int.box).toArray
    cSQL"#$columnName::int[] && $partiesArray::int[]"
  }

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"$elementColumnName = any($arrayColumnName)"

  override def isTrue(booleanColumnName: String): String = booleanColumnName

  override def constBooleanSelect(value: Boolean): String = if (value) "true" else "false"

  override def constBooleanWhere(value: Boolean): String = if (value) "true" else "false"

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
}
