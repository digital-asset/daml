// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.appendonlydao.events.Party
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy

object PostgresEventStrategy extends EventStrategy {
  override def filteredEventWitnessesClause(
      witnessesColumnName: String,
      parties: Set[Party],
  ): CompositeSql =
    if (parties.size == 1)
      cSQL"array[${parties.head.toString}]::text[]"
    else {
      val partiesArray: Array[String] = parties.view.map(_.toString).toArray
      cSQL"array(select unnest(#$witnessesColumnName) intersect select unnest($partiesArray::text[]))"
    }

  override def submittersArePartiesClause(
      submittersColumnName: String,
      parties: Set[Party],
  ): CompositeSql = {
    val partiesArray = parties.view.map(_.toString).toArray
    cSQL"(#$submittersColumnName::text[] && $partiesArray::text[])"
  }

  override def witnessesWhereClause(
      witnessesColumnName: String,
      filterParams: FilterParams,
  ): CompositeSql = {
    val wildCardClause = filterParams.wildCardParties match {
      case wildCardParties if wildCardParties.isEmpty =>
        Nil

      case wildCardParties =>
        val partiesArray = wildCardParties.view.map(_.toString).toArray
        cSQL"(#$witnessesColumnName::text[] && $partiesArray::text[])" :: Nil
    }
    val partiesTemplatesClauses =
      filterParams.partiesAndTemplates.iterator.map { case (parties, templateIds) =>
        val partiesArray = parties.view.map(_.toString).toArray
        val templateIdsArray = templateIds.view.map(_.toString).toArray
        cSQL"( (#$witnessesColumnName::text[] && $partiesArray::text[]) AND (template_id = ANY($templateIdsArray::text[])) )"
      }.toList
    (wildCardClause ::: partiesTemplatesClauses).mkComposite("(", " OR ", ")")
  }
}
