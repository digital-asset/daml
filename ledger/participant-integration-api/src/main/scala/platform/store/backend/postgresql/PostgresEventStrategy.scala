// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import anorm.{Row, SimpleSql}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy

object PostgresEventStrategy extends EventStrategy {

  override def wildcardPartiesClause(
      witnessesColumnName: String,
      internedWildcardParties: Set[Int],
  ): CompositeSql = {
    // anorm does not like primitive arrays, so we need to box it
    val internedWildcardPartiesArray = internedWildcardParties.map(Int.box).toArray
    cSQL"(#$witnessesColumnName::integer[] && $internedWildcardPartiesArray::integer[])"
  }

  override def partiesAndTemplatesClause(
      witnessesColumnName: String,
      internedParties: Set[Int],
      internedTemplates: Set[Int],
  ): CompositeSql = {
    // anorm does not like primitive arrays, so we need to box it
    val partiesArray = internedParties.map(Int.box).toArray
    val templateIdsArray = internedTemplates.map(Int.box).toArray
    cSQL"( (#$witnessesColumnName::integer[] && $partiesArray::integer[]) AND (template_id = ANY($templateIdsArray::integer[])) )"
  }

  override def pruneCreateFilters(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
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
