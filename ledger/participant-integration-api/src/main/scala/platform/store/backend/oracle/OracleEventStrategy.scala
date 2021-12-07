// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  override def filterPartiesClause(
      witnessesColumnName: String,
      internedPartiesTemplates: List[(Set[Int], Set[Int])],
  ): List[CompositeSql] = {
    internedPartiesTemplates
      .map { case (parties, templateIds) =>
        val clause =
          OracleQueryStrategy.arrayIntersectionNonEmptyClause(
            witnessesColumnName,
            parties,
          )
        // TODO: Postgres and H2 use Array[Integer], Oracle uses Set[Int] to send the template Ids.
        // Does it make a difference?
        cSQL"( ($clause) AND (template_id IN ($templateIds)) )"
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
