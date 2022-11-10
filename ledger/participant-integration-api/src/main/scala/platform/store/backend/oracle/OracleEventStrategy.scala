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

  override def pruneFilterCreateStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] = {
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

  override def pruneFilterCreateNonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = ???

  override def pruneFilterConsumingStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row] = ???

  override def pruneFilterConsumingNonStakeholderInformees(
      pruneUpToInclusive: Offset
  ): SimpleSql[Row] = ???

  override def pruneFilterNonConsumingInformees(pruneUpToInclusive: Offset): SimpleSql[Row] = ???

  override def pruneTransactionMeta(pruneUpToInclusive: Offset): SimpleSql[Row] = ???
}
