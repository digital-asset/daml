// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.RowParser
import anorm.SqlParser.long
import com.digitalasset.canton.platform.store.backend.Conversions.offset
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*

import java.sql.Connection

/** Contains dto classes each holding a minimal set of data sufficient to uniquely identify a row in
  * the corresponding table.
  */
object PruningDto {

  final case class TxMeta(offset: Long)
  final case class Completion(offset: Long)

}
class PruningDtoQueries {
  import PruningDto.*
  private def offsetParser[T](f: Long => T): RowParser[T] =
    offset("ledger_offset").map(_.unwrap) map f

  def eventActivate(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_events_activate_contract ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)
  def eventDeactivate(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_events_deactivate_contract ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)
  def eventVariousWitnessed(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_events_various_witnessed ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)

  def filterActivateStakeholder(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_filter_activate_stakeholder ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)
  def filterActivateWitness(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_filter_activate_witness ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)
  def filterDeactivateStakeholder(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_filter_deactivate_stakeholder ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)
  def filterDeactivateWitness(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_filter_deactivate_witness ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)
  def filterVariousWitness(implicit c: Connection): Seq[Long] =
    SQL"SELECT event_sequential_id FROM lapi_filter_various_witness ORDER BY event_sequential_id"
      .asVectorOf(long("event_sequential_id"))(c)

  def updateMeta(implicit c: Connection): Seq[TxMeta] =
    SQL"SELECT event_offset AS ledger_offset FROM lapi_update_meta ORDER BY event_offset"
      .asVectorOf(offsetParser(TxMeta.apply))(c)
  def completions(implicit c: Connection): Seq[Completion] =
    SQL"SELECT completion_offset AS ledger_offset FROM lapi_command_completions ORDER BY completion_offset"
      .asVectorOf(offsetParser(Completion.apply))(c)

}
