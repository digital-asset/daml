// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import anorm.{Row, SimpleSql}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.backend.common.EventStrategy

object OracleEventStrategy extends EventStrategy {

  override def wildcardPartiesClause(
      witnessesColumnName: String,
      internedWildcardParties: Set[Int],
  ): CompositeSql = {
    cSQL"(${OracleQueryStrategy.arrayIntersectionNonEmptyClause(witnessesColumnName, internedWildcardParties)})"
  }

  override def partiesAndTemplatesClause(
      witnessesColumnName: String,
      internedParties: Set[Int],
      internedTemplates: Set[Int],
  ): CompositeSql = {
    val clause =
      OracleQueryStrategy.arrayIntersectionNonEmptyClause(
        witnessesColumnName,
        internedParties,
      )
    cSQL"( ($clause) AND (template_id IN ($internedTemplates)) )"
  }

  override def pruneIdFilterCreateStakeholder(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterCreate(
      tableName = "pe_create_id_filter_stakeholder",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterCreateNonStakeholderInformee(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] =
    pruneIdFilterCreate(
      tableName = "pe_create_id_filter_non_stakeholder_informee",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterConsumingStakeholder(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_consuming_id_filter_stakeholder",
      eventsTableName = "participant_events_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

  override def pruneIdFilterConsumingNonStakeholderInformee(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = {
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_consuming_id_filter_non_stakeholder_informee",
      eventsTableName = "participant_events_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )
  }

  override def pruneIdFilterNonConsumingInformee(pruneUpToInclusive: Offset): SimpleSql[Row] =
    pruneIdFilterConsumingOrNonConsuming(
      idFilterTableName = "pe_non_consuming_id_filter_informee",
      eventsTableName = "participant_events_non_consuming_exercise",
      pruneUpToInclusive = pruneUpToInclusive,
    )

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

  // TODO etq: Currently we query two additional tables: create and consuming events tables.
  //           This can be simplified to query only the create events table if we impose the ordering
  //           that create events tables has already been pruned.
  /** Prunes create events id filter table only for contracts archived before the specified offset
    */
  private def pruneIdFilterCreate(tableName: String, pruneUpToInclusive: Offset): SimpleSql[Row] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
          DELETE FROM
            #$tableName id_filter
          WHERE EXISTS (
            SELECT * from participant_events_create c
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
            c.event_sequential_id = id_filter.event_sequential_id
          )"""
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
          WHERE EXISTS (
            SELECT * FROM #$eventsTableName events
          WHERE
            events.event_offset <= $pruneUpToInclusive
            AND
            events.event_sequential_id = id_filter.event_sequential_id
          )"""

  }
}
