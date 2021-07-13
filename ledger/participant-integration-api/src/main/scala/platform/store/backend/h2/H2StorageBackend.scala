// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection
import java.time.Instant

import anorm.SQL
import anorm.SqlStringInterpolation
import anorm.SqlParser.get
import com.daml.ledger.{ApplicationId, TransactionId}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.platform.store.appendonlydao.events.{ContractId, EventsTable, Key, Raw}
import com.daml.platform.store.backend.common.{
  AppendOnlySchema,
  CommonStorageBackend,
  TemplatedStorageBackend,
}
import TemplatedStorageBackend.limitClause
import com.daml.platform.store.backend.{DbDto, StorageBackend}

private[backend] object H2StorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with CommonStorageBackend[AppendOnlySchema.Batch] {

  override def reset(connection: Connection): Unit = {
    SQL("""set referential_integrity false;
        |truncate table configuration_entries;
        |truncate table package_entries;
        |truncate table parameters;
        |truncate table participant_command_completions;
        |truncate table participant_command_submissions;
        |truncate table participant_events_divulgence;
        |truncate table participant_events_create;
        |truncate table participant_events_consuming_exercise;
        |truncate table participant_events_non_consuming_exercise;
        |truncate table parties;
        |truncate table party_entries;
        |set referential_integrity true;""".stripMargin)
      .execute()(connection)
    ()
  }

  override def enforceSynchronousCommit(connection: Connection): Unit = () // Not supported

  override def duplicateKeyError: String = "Unique index or primary key violation"

  val SQL_INSERT_COMMAND: String =
    """merge into participant_command_submissions pcs
      |using dual on deduplication_key = {deduplicationKey}
      |when not matched then
      |  insert (deduplication_key, deduplicate_until)
      |  values ({deduplicationKey}, {deduplicateUntil})
      |when matched and pcs.deduplicate_until < {submittedAt} then
      |  update set deduplicate_until={deduplicateUntil}""".stripMargin

  def upsertDeduplicationEntry(
      key: String,
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(connection: Connection): Int =
    SQL(SQL_INSERT_COMMAND)
      .on(
        "deduplicationKey" -> key,
        "submittedAt" -> submittedAt,
        "deduplicateUntil" -> deduplicateUntil,
      )
      .executeUpdate()(connection)

  override def batch(dbDtos: Vector[DbDto]): AppendOnlySchema.Batch =
    H2Schema.schema.prepareData(dbDtos)

  override def insertBatch(connection: Connection, batch: AppendOnlySchema.Batch): Unit =
    H2Schema.schema.executeUpdate(batch, connection)

  def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(connection: Connection): List[CompletionStreamResponse] =
    TemplatedStorageBackend.commandCompletions(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      applicationId = applicationId,
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", parties),
    )(connection)

  def activeContractWithArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[StorageBackend.RawContract] =
    TemplatedStorageBackend.activeContractWithArgument(
      treeEventWitnessesWhereClause = arrayIntersectionWhereClause("tree_event_witnesses", readers),
      contractId = contractId,
    )(connection)

  def activeContractWithoutArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[String] =
    TemplatedStorageBackend.activeContractWithoutArgument(
      treeEventWitnessesWhereClause = arrayIntersectionWhereClause("tree_event_witnesses", readers),
      contractId = contractId,
    )(connection)

  def contractKey(readers: Set[Ref.Party], key: Key)(
      connection: Connection
  ): Option[ContractId] =
    TemplatedStorageBackend.contractKey(
      flatEventWitnesses = columnPrefix =>
        arrayIntersectionWhereClause(s"$columnPrefix.flat_event_witnesses", readers),
      key = key,
    )(connection)

  def transactionsEventsSingleWildcardParty(
      startExclusive: Long,
      endInclusive: Long,
      party: Ref.Party,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.transactionsEventsSingleWildcardParty(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      party = party,
      partyArrayContext = partyArrayContext,
      witnessesWhereClause = arrayIntersectionWhereClause("flat_event_witnesses", Set(party)),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def transactionsEventsSinglePartyWithTemplates(
      startExclusive: Long,
      endInclusive: Long,
      party: Ref.Party,
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.transactionsEventsSinglePartyWithTemplates(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      party = party,
      partyArrayContext = partyArrayContext,
      witnessesWhereClause = arrayIntersectionWhereClause("flat_event_witnesses", Set(party)),
      templateIds = templateIds,
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def transactionsEventsOnlyWildcardParties(
      startExclusive: Long,
      endInclusive: Long,
      parties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.transactionsEventsOnlyWildcardParties(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      filteredWitnessesClause = arrayIntersectionValues("flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", parties),
      witnessesWhereClause = arrayIntersectionWhereClause("flat_event_witnesses", parties),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)

  def transactionsEventsSameTemplates(
      startExclusive: Long,
      endInclusive: Long,
      parties: Set[Ref.Party],
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.transactionsEventsSameTemplates(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      filteredWitnessesClause = arrayIntersectionValues("flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", parties),
      witnessesWhereClause = arrayIntersectionWhereClause("flat_event_witnesses", parties),
      templateIds = templateIds,
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)

  def transactionsEventsMixedTemplates(
      startExclusive: Long,
      endInclusive: Long,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    val parties = partiesAndTemplateIds.map(_._1)
    TemplatedStorageBackend.transactionsEventsMixedTemplates(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      filteredWitnessesClause = arrayIntersectionValues("flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", parties),
      partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause("flat_event_witnesses", partiesAndTemplateIds),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)
  }

  def transactionsEventsMixedTemplatesWithWildcardParties(
      startExclusive: Long,
      endInclusive: Long,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      wildcardParties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
    TemplatedStorageBackend.transactionsEventsMixedTemplatesWithWildcardParties(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      filteredWitnessesClause = arrayIntersectionValues("flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", parties),
      witnessesWhereClause = arrayIntersectionWhereClause("flat_event_witnesses", wildcardParties),
      partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause("flat_event_witnesses", partiesAndTemplateIds),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)
  }

  def activeContractsEventsSingleWildcardParty(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      party: Ref.Party,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.activeContractsEventsSingleWildcardParty(
      startExclusive = startExclusive,
      endInclusiveSeq = endInclusiveSeq,
      endInclusiveOffset = endInclusiveOffset,
      party = party,
      partyArrayContext = partyArrayContext,
      witnessesWhereClause =
        arrayIntersectionWhereClause("active_cs.flat_event_witnesses", Set(party)),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def activeContractsEventsSinglePartyWithTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      party: Ref.Party,
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.activeContractsEventsSinglePartyWithTemplates(
      startExclusive = startExclusive,
      endInclusiveSeq = endInclusiveSeq,
      endInclusiveOffset = endInclusiveOffset,
      party = party,
      partyArrayContext = partyArrayContext,
      templateIds = templateIds,
      witnessesWhereClause =
        arrayIntersectionWhereClause("active_cs.flat_event_witnesses", Set(party)),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def activeContractsEventsOnlyWildcardParties(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      parties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.activeContractsEventsOnlyWildcardParties(
      startExclusive = startExclusive,
      endInclusiveSeq = endInclusiveSeq,
      endInclusiveOffset = endInclusiveOffset,
      filteredWitnessesClause = arrayIntersectionValues("active_cs.flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("active_cs.submitters", parties),
      witnessesWhereClause =
        arrayIntersectionWhereClause("active_cs.flat_event_witnesses", parties),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)

  def activeContractsEventsSameTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      parties: Set[Ref.Party],
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.activeContractsEventsSameTemplates(
      startExclusive = startExclusive,
      endInclusiveSeq = endInclusiveSeq,
      endInclusiveOffset = endInclusiveOffset,
      templateIds = templateIds,
      filteredWitnessesClause = arrayIntersectionValues("active_cs.flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("active_cs.submitters", parties),
      witnessesWhereClause =
        arrayIntersectionWhereClause("active_cs.flat_event_witnesses", parties),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)

  def activeContractsEventsMixedTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    val parties = partiesAndTemplateIds.map(_._1)
    TemplatedStorageBackend.activeContractsEventsMixedTemplates(
      startExclusive = startExclusive,
      endInclusiveSeq = endInclusiveSeq,
      endInclusiveOffset = endInclusiveOffset,
      filteredWitnessesClause = arrayIntersectionValues("active_cs.flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("active_cs.submitters", parties),
      partiesAndTemplatesCondition = formatPartiesAndTemplatesWhereClause(
        "active_cs.flat_event_witnesses",
        partiesAndTemplateIds,
      ),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)
  }

  def activeContractsEventsMixedTemplatesWithWildcardParties(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      wildcardParties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
    TemplatedStorageBackend.activeContractsEventsMixedTemplatesWithWildcardParties(
      startExclusive = startExclusive,
      endInclusiveSeq = endInclusiveSeq,
      endInclusiveOffset = endInclusiveOffset,
      filteredWitnessesClause = arrayIntersectionValues("active_cs.flat_event_witnesses", parties),
      submittersInPartiesClause = arrayIntersectionWhereClause("active_cs.submitters", parties),
      witnessesWhereClause =
        arrayIntersectionWhereClause("active_cs.flat_event_witnesses", wildcardParties),
      partiesAndTemplatesCondition = formatPartiesAndTemplatesWhereClause(
        "active_cs.flat_event_witnesses",
        partiesAndTemplateIds,
      ),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
    )(connection)
  }

  def flatTransactionSingleParty(
      transactionId: TransactionId,
      requestingParty: Ref.Party,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.flatTransactionSingleParty(
      transactionId = transactionId,
      requestingParty = requestingParty,
      partyArrayContext = partyArrayContext,
      witnessesWhereClause =
        arrayIntersectionWhereClause("flat_event_witnesses", Set(requestingParty)),
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def flatTransactionMultiParty(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    TemplatedStorageBackend.flatTransactionMultiParty(
      transactionId = transactionId,
      witnessesWhereClause =
        arrayIntersectionWhereClause("flat_event_witnesses", requestingParties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", requestingParties),
    )(connection)

  def transactionTreeSingleParty(
      transactionId: TransactionId,
      requestingParty: Ref.Party,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] =
    TemplatedStorageBackend.transactionTreeSingleParty(
      transactionId = transactionId,
      requestingParty = requestingParty,
      partyArrayContext = partyArrayContext,
      witnessesWhereClause =
        arrayIntersectionWhereClause("tree_event_witnesses", Set(requestingParty)),
      createEventFilter = columnEqualityBoolean("event_kind", "20"),
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def transactionTreeMultiParty(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] =
    TemplatedStorageBackend.transactionTreeMultiParty(
      transactionId = transactionId,
      witnessesWhereClause =
        arrayIntersectionWhereClause("tree_event_witnesses", requestingParties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", requestingParties),
      filteredWitnessesClause = arrayIntersectionValues("tree_event_witnesses", requestingParties),
      createEventFilter = columnEqualityBoolean("event_kind", "20"),
    )(connection)

  def transactionTreeEventsSingleParty(
      startExclusive: Long,
      endInclusive: Long,
      requestingParty: Ref.Party,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] =
    TemplatedStorageBackend.transactionTreeEventsSingleParty(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      requestingParty = requestingParty,
      partyArrayContext = partyArrayContext,
      witnessesWhereClause =
        arrayIntersectionWhereClause("tree_event_witnesses", Set(requestingParty)),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
      createEventFilter = columnEqualityBoolean("event_kind", "20"),
      submitterIsPartyClause = submittersIsPartyClause,
    )(connection)

  def transactionTreeEventsMultiParty(
      startExclusive: Long,
      endInclusive: Long,
      requestingParties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] =
    TemplatedStorageBackend.transactionTreeEventsMultiParty(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      witnessesWhereClause =
        arrayIntersectionWhereClause("tree_event_witnesses", requestingParties),
      filteredWitnessesClause = arrayIntersectionValues("tree_event_witnesses", requestingParties),
      submittersInPartiesClause = arrayIntersectionWhereClause("submitters", requestingParties),
      limitExpr = limitClause(limit),
      fetchSizeHint = fetchSizeHint,
      createEventFilter = columnEqualityBoolean("event_kind", "20"),
    )(connection)

  // TODO FIXME: this is for postgres not for H2
  def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc limit 1"
      .as(get[Long](1).singleOpt)(connection)
  }

  private def format(parties: Set[Party]): String = parties.view.map(p => s"'$p'").mkString(",")


  private def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Ref.Party]): String =
    if (parties.isEmpty)
      "false"
    else
      parties.view.map(p => s"array_contains($arrayColumn, '$p')").mkString("(", " or ", ")")

  private def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
    s"array_intersection($arrayColumn, array[${format(parties)}])"

  private def formatPartiesAndTemplatesWhereClause(
      witnessesAggregationColumn: String,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
  ): String =
    partiesAndTemplateIds.view
      .map { case (p, i) =>
        s"(${arrayIntersectionWhereClause(witnessesAggregationColumn, Set(p))} and template_id = '$i')"
      }
      .mkString("(", " or ", ")")

  private val partyArrayContext = ("array[", "]")

  private def columnEqualityBoolean(column: String, value: String) = s"""$column = $value"""

  private def submittersIsPartyClause(submittersColumnName: String): (String, String) =
    (s"$submittersColumnName = array[", "]")
}
