// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.ComposableQuery.CompositeSql
import com.daml.platform.store.backend.common.QueryStrategy

object PostgresQueryStrategy extends QueryStrategy {

  override def arrayIntersectionNonEmptyClause(
      columnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val partiesArray: Array[String] = parties.map(_.toString).toArray
    cSQL"#$columnName::text[] && $partiesArray::text[]"
  }

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"$elementColumnName = any($arrayColumnName)"

  override def isTrue(booleanColumnName: String): String = booleanColumnName
}
