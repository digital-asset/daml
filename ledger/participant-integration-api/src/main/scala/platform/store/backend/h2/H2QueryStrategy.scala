// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.QueryStrategy
import com.daml.platform.store.interning.StringInterning

object H2QueryStrategy extends QueryStrategy {

  override def arrayIntersectionNonEmptyClause(
      columnName: String,
      parties: Set[Ref.Party],
      stringInterning: StringInterning,
  ): CompositeSql = {
    val internedParties = parties.view
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.toList)
      .map(p => cSQL"array_contains(#$columnName, $p)")
      .toList
    if (internedParties.isEmpty)
      cSQL"false"
    else
      internedParties
        .mkComposite("(", " or ", ")")
  }

  override def arrayContains(arrayColumnName: String, elementColumnName: String): String =
    s"array_contains($arrayColumnName, $elementColumnName)"

  override def isTrue(booleanColumnName: String): String = booleanColumnName
}
