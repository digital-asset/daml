// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import anorm.{Row, SimpleSql}
import com.daml.ledger.offset.Offset
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
            .toArray // anorm does not like primitive arrays, so we need to box it
        if (templateIdsArray.isEmpty) Iterator.empty
        else Iterator(cSQL"( ($clause) AND (template_id = ANY($templateIdsArray)) )")
      }.toList
    wildCardClause ::: partiesTemplatesClauses match {
      case Nil => cSQL"false"
      case allClauses => allClauses.mkComposite("(", " OR ", ")")
    }
  }

  override def pruneCreateFilters(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""
          -- Create events filter table (only for contracts archived before the specified offset)
          delete from participant_events_create_filter
          where exists (
            select * from participant_events_create delete_events
            where
              delete_events.event_offset <= $pruneUpToInclusive and
              exists (
                SELECT 1 FROM participant_events_consuming_exercise archive_events
                WHERE
                  archive_events.event_offset <= $pruneUpToInclusive AND
                  archive_events.contract_id = delete_events.contract_id
              ) and
              delete_events.event_sequential_id = participant_events_create_filter.event_sequential_id
          )"""
  }
}
