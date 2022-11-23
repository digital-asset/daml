// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy

object H2EventStrategy extends EventStrategy {

  override def wildcardPartiesClause(
      witnessesColumnName: String,
      internedWildcardParties: Set[Int],
  ): CompositeSql = {
    cSQL"(${H2QueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, internedWildcardParties)})"
  }

  override def partiesAndTemplatesClause(
      witnessesColumnName: String,
      internedParties: Set[Int],
      internedTemplates: Set[Int],
  ): CompositeSql = {
    val clause =
      H2QueryStrategy.arrayIntersectionNonEmptyClause(
        witnessesColumnName,
        internedParties,
      )
    // anorm does not like primitive arrays, so we need to box it
    val templateIdsArray = internedTemplates.map(Int.box).toArray

    cSQL"( ($clause) AND (template_id = ANY($templateIdsArray)) )"
  }

}
