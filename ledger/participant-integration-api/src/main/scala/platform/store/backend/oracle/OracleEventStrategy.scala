// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy

object OracleEventStrategy extends EventStrategy {

  override def filteredEventWitnessesClause(
      witnessesColumnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql =
    if (parties.size == 1)
      cSQL"(json_array(${parties.head.toString}))"
    else
      cSQL"""
           (select json_arrayagg(value) from (select value
           from json_table(#$witnessesColumnName, '$$[*]' columns (value PATH '$$'))
           where value IN (${parties.map(_.toString)})))
           """

  override def submittersArePartiesClause(
      submittersColumnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql =
    cSQL"(${OracleQueryStrategy.arrayIntersectionNonEmptyClause(submittersColumnName, parties)})"

  override def witnessesWhereClause(
      witnessesColumnName: String,
      filterParams: FilterParams,
  ): CompositeSql = {
    val wildCardClause = filterParams.wildCardParties match {
      case wildCardParties if wildCardParties.isEmpty =>
        Nil

      case wildCardParties =>
        cSQL"(${OracleQueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, wildCardParties)})" :: Nil
    }
    val partiesTemplatesClauses =
      filterParams.partiesAndTemplates.iterator.map { case (parties, templateIds) =>
        val clause =
          OracleQueryStrategy.arrayIntersectionNonEmptyClause(
            witnessesColumnName,
            parties,
          )
        cSQL"( ($clause) AND (template_id IN (${templateIds.map(_.toString)})) )"
      }.toList
    (wildCardClause ::: partiesTemplatesClauses).mkComposite("(", " OR ", ")")
  }
}
