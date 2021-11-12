// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy
import com.daml.platform.store.interning.StringInterning

object H2EventStrategy extends EventStrategy {
  override def filteredEventWitnessesClause(
      witnessesColumnName: String,
      parties: Set[Ref.Party],
      stringInterning: StringInterning,
  ): CompositeSql = {
    val partiesArray: Array[java.lang.Integer] =
      parties.view.map(stringInterning.party.tryInternalize).flatMap(_.toList).map(Int.box).toArray
    if (partiesArray.isEmpty) cSQL"false"
    else cSQL"array_intersection(#$witnessesColumnName, $partiesArray)"
  }

  override def submittersArePartiesClause(
      submittersColumnName: String,
      parties: Set[Ref.Party],
      stringInterning: StringInterning,
  ): CompositeSql =
    H2QueryStrategy.arrayIntersectionNonEmptyClause(
      columnName = submittersColumnName,
      parties = parties,
      stringInterning = stringInterning,
    )

  override def witnessesWhereClause(
      witnessesColumnName: String,
      filterParams: FilterParams,
      stringInterning: StringInterning,
  ): CompositeSql = {
    val wildCardClause = filterParams.wildCardParties match {
      case wildCardParties if wildCardParties.isEmpty =>
        Nil

      case wildCardParties =>
        cSQL"(${H2QueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, wildCardParties, stringInterning)})" :: Nil
    }
    val partiesTemplatesClauses =
      filterParams.partiesAndTemplates.iterator.flatMap { case (parties, templateIds) =>
        val clause =
          H2QueryStrategy.arrayIntersectionNonEmptyClause(
            witnessesColumnName,
            parties,
            stringInterning,
          )
        val templateIdsArray: Array[java.lang.Integer] =
          templateIds.view
            .map(stringInterning.templateId.tryInternalize)
            .flatMap(_.toList)
            .map(Int.box)
            .toArray
        if (templateIdsArray.isEmpty) Iterator.empty
        else Iterator(cSQL"( ($clause) AND (template_id = ANY($templateIdsArray)) )")
      }.toList
    wildCardClause ::: partiesTemplatesClauses match {
      case Nil => cSQL"false"
      case allClauses => allClauses.mkComposite("(", " OR ", ")")
    }
  }
}
