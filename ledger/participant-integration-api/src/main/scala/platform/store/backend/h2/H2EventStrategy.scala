// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy

object H2EventStrategy extends EventStrategy {
  override def filteredEventWitnessesClause(
      witnessesColumnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql = {
    val partiesArray = parties.view.map(_.toString).toArray
    cSQL"array_intersection(#$witnessesColumnName, $partiesArray)"
  }

  override def submittersArePartiesClause(
      submittersColumnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql =
    H2QueryStrategy.arrayIntersectionNonEmptyClause(
      columnName = submittersColumnName,
      parties = parties,
    )

  override def witnessesWhereClause(
      witnessesColumnName: String,
      filterParams: FilterParams,
  ): CompositeSql = {
    val wildCardClause = filterParams.wildCardParties match {
      case wildCardParties if wildCardParties.isEmpty =>
        Nil

      case wildCardParties =>
        cSQL"(${H2QueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, wildCardParties)})" :: Nil
    }
    val partiesTemplatesClauses =
      filterParams.partiesAndTemplates.iterator.map { case (parties, templateIds) =>
        val clause =
          H2QueryStrategy.arrayIntersectionNonEmptyClause(
            witnessesColumnName,
            parties,
          )
        val templateIdsArray = templateIds.view.map(_.toString).toArray
        cSQL"( ($clause) AND (template_id = ANY($templateIdsArray)) )"
      }.toList
    (wildCardClause ::: partiesTemplatesClauses).mkComposite("(", " OR ", ")")
  }
}
