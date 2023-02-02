// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import anorm.RowParser
import anorm.SqlParser.{long, str}
import anorm.~
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._

/** Contains dto classes each holding a minimal set of data sufficient
  * to uniquely identify a row in the corresponding table.
  */
object PruningDto {

  case class EventCreate(seqId: Long)
  case class EventConsuming(seqId: Long)
  case class EventNonConsuming(seqId: Long)
  case class EventDivulgence(seqId: Long)

  case class FilterCreateStakeholder(seqId: Long, party: Long)
  case class FilterCreateNonStakeholder(seqId: Long, party: Long)
  case class FilterConsumingStakeholder(seqId: Long, party: Long)
  case class FilterConsumingNonStakeholder(seqId: Long, party: Long)
  case class FilterNonConsuming(seqId: Long, party: Long)

  case class TxMeta(offset: String)
  case class Completion(offset: String)

}
class PruningDtoQueries {
  import PruningDto._
  private def seqIdParser[T](f: Long => T): RowParser[T] = long("event_sequential_id").map(f)
  private def idFilterParser[T](f: (Long, Long) => T): RowParser[T] =
    long("event_sequential_id") ~ long("party_id") map { case seqId ~ partyId => f(seqId, partyId) }
  private def offsetParser[T](f: String => T): RowParser[T] = str("offset") map (f)

  def eventCreate(implicit c: Connection): Seq[EventCreate] =
    SQL"SELECT event_sequential_id FROM participant_events_create ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventCreate))(c)
  def eventConsuming(implicit c: Connection): Seq[EventConsuming] =
    SQL"SELECT event_sequential_id FROM participant_events_consuming_exercise ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventConsuming))(c)
  def eventNonConsuming(implicit c: Connection): Seq[EventNonConsuming] =
    SQL"SELECT event_sequential_id FROM participant_events_non_consuming_exercise ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventNonConsuming))(c)
  def eventDivulgence(implicit c: Connection): Seq[EventDivulgence] =
    SQL"SELECT event_sequential_id FROM participant_events_divulgence ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(EventDivulgence))(c)

  def filterCreateStakeholder(implicit c: Connection): Seq[FilterCreateStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_create_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterCreateStakeholder))(c)
  def filterCreateNonStakeholder(implicit c: Connection): Seq[FilterCreateNonStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_create_id_filter_non_stakeholder_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterCreateNonStakeholder))(c)
  def filterConsumingStakeholder(implicit c: Connection): Seq[FilterConsumingStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_consuming_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterConsumingStakeholder))(c)
  def filterConsumingNonStakeholder(implicit c: Connection): Seq[FilterConsumingNonStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_consuming_id_filter_non_stakeholder_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterConsumingNonStakeholder))(c)
  def filterNonConsuming(implicit c: Connection): Seq[FilterNonConsuming] =
    SQL"SELECT event_sequential_id, party_id FROM pe_non_consuming_id_filter_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(FilterNonConsuming))(c)

  def txMeta(implicit c: Connection): Seq[TxMeta] =
    SQL"SELECT event_offset AS offset FROM participant_transaction_meta ORDER BY event_offset"
      .asVectorOf(offsetParser(TxMeta))(c)
  def completions(implicit c: Connection): Seq[Completion] =
    SQL"SELECT completion_offset AS offset FROM participant_command_completions ORDER BY completion_offset"
      .asVectorOf(offsetParser(Completion))(c)

}
