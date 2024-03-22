// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy

object H2QueryStrategy extends QueryStrategy {

  override def arrayIntersectionNonEmptyClause(
      columnName: String,
      internedParties: Set[Int],
  ): CompositeSql = {
    require(internedParties.nonEmpty, "internedParties must be non-empty")
    internedParties
      .map(p => cSQL"array_contains(#$columnName, $p)")
      .mkComposite("(", " or ", ")")
  }

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"array_contains($arrayColumnName, $elementColumnName)"

  override def isTrue(booleanColumnName: String): String = booleanColumnName

  override def constBooleanSelect(value: Boolean): String = if (value) "true" else "false"

  override def constBooleanWhere(value: Boolean): String = if (value) "true" else "false"
}
