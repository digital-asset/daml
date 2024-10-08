// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.SqlParser.long
import anorm.{RowParser, ~}
import com.digitalasset.canton.platform.store.backend.Conversions.offset
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*

import java.sql.Connection

/** Contains dto classes each holding a minimal set of data sufficient
  * to uniquely identify a row in the corresponding table.
  */
object PruningDto {

  final case class EventCreate(seqId: Long)
  final case class EventConsuming(seqId: Long)
  final case class EventNonConsuming(seqId: Long)
  final case class EventAssign(seqId: Long)
  final case class EventUnassign(seqId: Long)

  final case class FilterCreateStakeholder(seqId: Long, party: Long)
  final case class FilterCreateNonStakeholder(seqId: Long, party: Long)
  final case class FilterConsumingStakeholder(seqId: Long, party: Long)
  final case class FilterConsumingNonStakeholder(seqId: Long, party: Long)
  final case class FilterNonConsuming(seqId: Long, party: Long)
  final case class FilterAssign(seqId: Long, party: Long)
  final case class FilterUnassign(seqId: Long, party: Long)

  final case class TxMeta(offset: Long)
  final case class Completion(offset: Long)

}
class PruningDtoQueries {
  import PruningDto.*
  private def seqIdParser[T](f: Long => T): RowParser[T] = long("event_sequential_id").map(f)
  private def idFilterParser[T](f: (Long, Long) => T): RowParser[T] =
    long("event_sequential_id") ~ long("party_id") map { case seqId ~ partyId => f(seqId, partyId) }
  private def offsetParser[T](f: Long => T): RowParser[T] =
    offset("ledger_offset").map(_.toLong) map (f)

  def eventCreate(implicit c: Connection): Seq[EventCreate] =
    SQL"SELECT event_sequential_id FROM lapi_events_create ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventCreate.apply))(c)
  def eventConsuming(implicit c: Connection): Seq[EventConsuming] =
    SQL"SELECT event_sequential_id FROM lapi_events_consuming_exercise ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventConsuming.apply))(c)
  def eventNonConsuming(implicit c: Connection): Seq[EventNonConsuming] =
    SQL"SELECT event_sequential_id FROM lapi_events_non_consuming_exercise ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventNonConsuming.apply))(c)
  def eventAssign(implicit c: Connection): Seq[EventAssign] =
    SQL"SELECT event_sequential_id FROM lapi_events_assign ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventAssign.apply))(c)
  def eventUnassign(implicit c: Connection): Seq[EventUnassign] =
    SQL"SELECT event_sequential_id FROM lapi_events_unassign ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventUnassign.apply))(c)

  def filterCreateStakeholder(implicit c: Connection): Seq[FilterCreateStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_create_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterCreateStakeholder.apply))(c)
  def filterCreateNonStakeholder(implicit c: Connection): Seq[FilterCreateNonStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_create_id_filter_non_stakeholder_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterCreateNonStakeholder.apply))(c)
  def filterConsumingStakeholder(implicit c: Connection): Seq[FilterConsumingStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_consuming_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterConsumingStakeholder.apply))(c)
  def filterConsumingNonStakeholder(implicit c: Connection): Seq[FilterConsumingNonStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_consuming_id_filter_non_stakeholder_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterConsumingNonStakeholder.apply))(c)
  def filterNonConsuming(implicit c: Connection): Seq[FilterNonConsuming] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_non_consuming_id_filter_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterNonConsuming.apply))(c)
  def filterAssign(implicit c: Connection): Seq[FilterAssign] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_assign_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterAssign.apply))(c)
  def filterUnassign(implicit c: Connection): Seq[FilterUnassign] =
    SQL"SELECT event_sequential_id, party_id FROM lapi_pe_unassign_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterUnassign.apply))(c)

  def txMeta(implicit c: Connection): Seq[TxMeta] =
    SQL"SELECT event_offset AS ledger_offset FROM lapi_transaction_meta ORDER BY event_offset"
      .asVectorOf(offsetParser(TxMeta.apply))(c)
  def completions(implicit c: Connection): Seq[Completion] =
    SQL"SELECT completion_offset AS ledger_offset FROM lapi_command_completions ORDER BY completion_offset"
      .asVectorOf(offsetParser(Completion.apply))(c)

}
