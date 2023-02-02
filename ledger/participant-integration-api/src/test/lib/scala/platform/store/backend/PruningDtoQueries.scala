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

  case class Create(seqId: Long)
  case class Consuming(seqId: Long)
  case class NonConsuming(seqId: Long)
  case class Divulgence(seqId: Long)

  case class CreateFilterStakeholder(seqId: Long, party: Long)
  case class CreateFilterNonStakeholder(seqId: Long, party: Long)
  case class ConsumingFilterStakeholder(seqId: Long, party: Long)
  case class ConsumingFilterNonStakeholder(seqId: Long, party: Long)
  case class NonConsumingFilter(seqId: Long, party: Long)

  case class TxMeta(offset: String)
  case class Completion(offset: String)

}
class PruningDtoQueries {
  import PruningDto._
  private def seqIdParser[T](f: Long => T): RowParser[T] = long("event_sequential_id").map(f)
  private def idFilterParser[T](f: (Long, Long) => T): RowParser[T] =
    long("event_sequential_id") ~ long("party_id") map { case seqId ~ partyId => f(seqId, partyId) }
  private def offsetParser[T](f: String => T): RowParser[T] = str("offset") map (f)

  def create(implicit c: Connection): Seq[Create] =
    SQL"SELECT event_sequential_id FROM participant_events_create ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(Create))(c)
  def consuming(implicit c: Connection): Seq[Consuming] =
    SQL"SELECT event_sequential_id FROM participant_events_consuming_exercise ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(Consuming))(c)
  def nonConsuming(implicit c: Connection): Seq[NonConsuming] =
    SQL"SELECT event_sequential_id FROM participant_events_non_consuming_exercise ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(NonConsuming))(c)
  def divulgence(implicit c: Connection): Seq[Divulgence] =
    SQL"SELECT event_sequential_id FROM participant_events_divulgence ORDER BY event_sequential_id"
      .asVectorOf(seqIdParser(Divulgence))(c)

  def createFilterStakeholder(implicit c: Connection): Seq[CreateFilterStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_create_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(CreateFilterStakeholder))(c)
  def createFilterNonStakeholder(implicit c: Connection): Seq[CreateFilterNonStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_create_id_filter_non_stakeholder_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(CreateFilterNonStakeholder))(c)
  def consumingFilterStakeholder(implicit c: Connection): Seq[ConsumingFilterStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_consuming_id_filter_stakeholder ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(ConsumingFilterStakeholder))(c)
  def consumingFilterNonStakeholder(implicit c: Connection): Seq[ConsumingFilterNonStakeholder] =
    SQL"SELECT event_sequential_id, party_id FROM pe_consuming_id_filter_non_stakeholder_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(ConsumingFilterNonStakeholder))(c)
  def nonConsumingFilter(implicit c: Connection): Seq[NonConsumingFilter] =
    SQL"SELECT event_sequential_id, party_id FROM pe_non_consuming_id_filter_informee ORDER BY event_sequential_id, party_id"
      .asVectorOf(idFilterParser(NonConsumingFilter))(c)

  def txMeta(implicit c: Connection): Seq[TxMeta] =
    SQL"SELECT event_offset AS offset FROM participant_transaction_meta ORDER BY event_offset"
      .asVectorOf(offsetParser(TxMeta))(c)
  def completions(implicit c: Connection): Seq[Completion] =
    SQL"SELECT completion_offset AS offset FROM participant_command_completions ORDER BY completion_offset"
      .asVectorOf(offsetParser(Completion))(c)

}
