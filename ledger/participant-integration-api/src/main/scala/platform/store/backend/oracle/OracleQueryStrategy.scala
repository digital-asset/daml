// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.QueryStrategy
import com.daml.platform.store.interning.StringInterning

object OracleQueryStrategy extends QueryStrategy {

  override def arrayIntersectionNonEmptyClause(
      columnName: String,
      parties: Set[Ref.Party],
      stringInterning: StringInterning,
  ): CompositeSql = {
    val internedParties =
      parties.view.map(stringInterning.party.tryInternalize).flatMap(_.toList).toSet
    if (internedParties.isEmpty) cSQL"1 = 0"
    else
      cSQL"(EXISTS (SELECT 1 FROM JSON_TABLE(#$columnName, '$$[*]' columns (value NUMBER PATH '$$')) WHERE value IN ($internedParties)))"
  }

  override def columnEqualityBoolean(column: String, value: String): String =
    s"""case when ($column = $value) then 1 else 0 end"""

  override def booleanOrAggregationFunction: String = "max"

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"EXISTS (SELECT 1 FROM JSON_TABLE($arrayColumnName, '$$[*]' columns (value NUMBER PATH '$$')) WHERE value = $elementColumnName)"

  override def isTrue(booleanColumnName: String): String = s"$booleanColumnName = 1"

  // WARNING! this construction will lead to "= ANY(?, ?, ?, ..... ?)" SQLs, for which oracle has an upper limit of 1000
  override def anyOf(longs: Iterable[Long]): CompositeSql = {
    val longArray: Vector[java.lang.Long] =
      longs.view.map(Long.box).toVector
    cSQL"= ANY($longArray)"
  }
}
