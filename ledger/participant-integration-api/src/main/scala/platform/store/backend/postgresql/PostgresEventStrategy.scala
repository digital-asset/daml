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

  override def pruneIdFilterCreateStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterCreate(
      tableName = "participant_events_create_filter",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterCreateNonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] =
    pruneIdFilterCreate(
      tableName = "pe_create_filter_nonstakeholder_informees",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterConsumingStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_consuming_exercise_filter_stakeholders",
      eventsTableName = "participant_events_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterConsumingNonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_consuming_exercise_filter_nonstakeholder_informees",
      eventsTableName = "participant_events_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterNonConsumingInformees(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_non_consuming_exercise_filter_informees",
      eventsTableName = "participant_events_non_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneTransactionMeta(pruneUpToInclusive: Offset): SimpleSql[Row] = {
    // For each transaction we check only if at least one of its create events still exists.
    // We don't have to check consuming or non-consuming events because we can assume these event tables have all the
    // events from the pruning range already deleted.
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

  // TODO etq: Currently we query two additional tables: create and consuming events tables.
  //           This can be simplified to query only the create events table if we impose the ordering
  //           that create events tables has already been pruned.
  /** Prunes create events id filter table only for contracts archived before the specified offset
    */
  private def pruneIdFilterCreate(tableName: String, pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            #$tableName filter
          USING participant_events_create c
          WHERE
            c.event_offset <= $pruneUpToInclusive
            AND
            EXISTS (
              SELECT 1 FROM participant_events_consuming_exercise archive
              WHERE
                archive.event_offset <= $pruneUpToInclusive
                AND
                archive.contract_id = c.contract_id
            )
            AND
            c.event_sequential_id = filter.event_sequential_id"""
  }

  // TODO etq: Currently we query an events table to discover the event offset corresponding
  //           to a row from the id filter table.
  //           This query can simplified not to query the events table at all if we pruned up to
  //           a given event sequential id rather than an event offset.
  private def pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName: String,
      eventsTableName: String,
      pruneUpToInclusive: Offset,
  ): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            #$idFilterTableName id_filter
          USING #$eventsTableName events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = id_filter.event_sequential_id"""
  }

}
