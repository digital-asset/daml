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

  // TODO etq: This query can be simplified if we can assume that pruning of event tables have already happened
  //           However maybe we are checking for existence of an archive event because that's more efficient?
  override def pruneFilterCreateStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] = {
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

  // TODO etq: This query can be simplified if we can assume that pruning of event tables have already happened?
  //           However maybe we are checking for existence of an archive event because that's more efficient?
  override def pruneFilterCreateNonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_create_filter_nonstakeholder_informees filter
          USING participant_events_create events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            EXISTS (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive
                AND
                archive_events.contract_id = events.contract_id
              )
            AND
            events.event_sequential_id = filter.event_sequential_id"""
  }

  override def pruneFilterConsumingStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_consuming_exercise_filter_stakeholders filter
          USING participant_events_consuming_exercise events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = filter.event_sequential_id"""
  }

  override def pruneFilterConsumingNonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_consuming_exercise_filter_nonstakeholder_informees filter
          USING participant_events_consuming_exercise events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = filter.event_sequential_id"""
  }

  override def pruneFilterNonConsumingInformees(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_non_consuming_exercise_filter_informees filter
          USING participant_events_non_consuming_exercise events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = filter.event_sequential_id"""
  }

  override def pruneTransactionMeta(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    // For each transaction we check only if at least one of its create events still exists
    // and don't have to check consuming or non-consuming events because we can assume event tables have already been pruned.
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
         DELETE FROM
            participant_transaction_meta m
         WHERE
          m.event_offset <= $pruneUpToInclusive
          AND
          NOT EXISTS (
            SELECT 1 FROM participant_events_create c
            WHERE
              c.event_sequential_id >= m.event_sequential_id_from
              AND
              c.event_sequential_id <= m.event_sequential_id_to
          )
       """
  }
}
