// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import anorm.{Row, SimpleSql}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.appendonlydao.events.Party
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy
import com.daml.platform.store.interning.StringInterning

object PostgresEventStrategy extends EventStrategy {
  override def filteredEventWitnessesClause(
      witnessesColumnName: String,
      parties: Set[Party],
      stringInterning: StringInterning,
  ): CompositeSql = {
    val internedParties: Array[java.lang.Integer] = parties.view
      .flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box).toList)
      .toArray
    if (internedParties.length == 1)
      cSQL"array[${internedParties.head}]::integer[]"
    else
      cSQL"array(select unnest(#$witnessesColumnName) intersect select unnest($internedParties::integer[]))"
  }

  override def submittersArePartiesClause(
      submittersColumnName: String,
      parties: Set[Party],
      stringInterning: StringInterning,
  ): CompositeSql = {
    val partiesArray: Array[java.lang.Integer] = parties.view
      .flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box).toList)
      .toArray
    cSQL"(#$submittersColumnName::integer[] && $partiesArray::integer[])"
  }

  override def witnessesWhereClause(
      witnessesColumnName: String,
      filterParams: FilterParams,
      stringInterning: StringInterning,
  ): CompositeSql = {
    val wildCardClause = filterParams.wildCardParties match {
      case wildCardParties if wildCardParties.isEmpty =>
        Nil

      case wildCardParties =>
        val partiesArray: Array[java.lang.Integer] = wildCardParties.view
          .flatMap(party => stringInterning.party.tryInternalize(party).map(Int.box).toList)
          .toArray
        if (partiesArray.isEmpty)
          Nil
        else
          cSQL"(#$witnessesColumnName::integer[] && $partiesArray::integer[])" :: Nil
    }
    val partiesTemplatesClauses =
      filterParams.partiesAndTemplates.iterator
        .map { case (parties, templateIds) =>
          (
            parties.flatMap(s => stringInterning.party.tryInternalize(s).toList),
            templateIds.flatMap(s => stringInterning.templateId.tryInternalize(s).toList),
          )
        }
        .filterNot(_._1.isEmpty)
        .filterNot(_._2.isEmpty)
        .map { case (parties, templateIds) =>
          val partiesArray: Array[java.lang.Integer] = parties.view.map(Int.box).toArray
          val templateIdsArray: Array[java.lang.Integer] =
            templateIds.view
              .map(Int.box)
              .toArray // anorm does not like primitive arrays, so we need to box it
          cSQL"( (#$witnessesColumnName::integer[] && $partiesArray::integer[]) AND (template_id = ANY($templateIdsArray::integer[])) )"
        }
        .toList

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
          using participant_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive AND
                archive_events.contract_id = delete_events.contract_id
            ) and
            delete_events.event_sequential_id = participant_events_create_filter.event_sequential_id"""
  }
}
