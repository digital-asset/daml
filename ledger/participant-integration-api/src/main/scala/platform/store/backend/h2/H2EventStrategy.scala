// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import anorm.{Row, SimpleSql}
import com.daml.ledger.offset.Offset
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

  override def pruneCreateFilters_stakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
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

  // TODO pbatko: test me
  override def pruneCreateFilters_nonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_create_filter_nonstakeholder_informees filter
          WHERE EXISTS (
            select * from participant_events_create events
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
            events.event_sequential_id = filter.event_sequential_id
          )"""
  }

  override def pruneConsumingFilters_stakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_consuming_exercise_filter_stakeholders filter
          WHERE EXISTS (
            select * from  participant_events_consuming_exercise events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = filter.event_sequential_id
          )"""
  }

  override def pruneConsumingFilters_nonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_consuming_exercise_filter_nonstakeholder_informees filter
          WHERE EXISTS (
            select * from  participant_events_consuming_exercise events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = filter.event_sequential_id
          )"""
  }

  override def pruneNonConsumingFilters_informees(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            pe_non_consuming_exercise_filter_informees filter
          WHERE EXISTS (
            select * from  participant_events_non_consuming_exercise events
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
            events.event_sequential_id = filter.event_sequential_id
          )"""
  }

  override def pruneTransactionMeta(pruneUpToInclusive: Offset): SimpleSql[Row] = {
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
