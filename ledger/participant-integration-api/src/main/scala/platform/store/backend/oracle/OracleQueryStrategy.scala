// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.QueryStrategy

object OracleQueryStrategy extends QueryStrategy {

  override def arrayIntersectionNonEmptyClause(
      columnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql =
    cSQL"EXISTS (SELECT 1 FROM JSON_TABLE(#$columnName, '$$[*]' columns (value PATH '$$')) WHERE value IN (${parties
      .map(_.toString)}))"

  override def columnEqualityBoolean(column: String, value: String): String =
    s"""case when ($column = $value) then 1 else 0 end"""

  override def booleanOrAggregationFunction: String = "max"

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"EXISTS (SELECT 1 FROM JSON_TABLE($arrayColumnName, '$$[*]' columns (value PATH '$$')) WHERE value = $elementColumnName)"

  override def isTrue(booleanColumnName: String): String = s"$booleanColumnName = 1"
}
