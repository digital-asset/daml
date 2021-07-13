// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import anorm.SqlParser.get
import anorm.{NamedParameter, SQL, SqlStringInterpolation}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.ApplicationId
import com.daml.lf.data.Ref
import com.daml.platform.store.appendonlydao.events.{ContractId, Key}
import com.daml.platform.store.backend.common.{
  AppendOnlySchema,
  CommonStorageBackend,
  EventStorageBackendTemplate,
  EventStrategy,
  TemplatedStorageBackend,
}
import TemplatedStorageBackend.limitClause
import com.daml.platform.store.backend.{DbDto, StorageBackend}

import java.sql.Connection
import java.time.Instant

import com.daml.platform.store.backend.EventStorageBackend.FilterParams

private[backend] object OracleStorageBackend
    extends StorageBackend[AppendOnlySchema.Batch]
    with CommonStorageBackend[AppendOnlySchema.Batch]
    with EventStorageBackendTemplate {

  override def reset(connection: Connection): Unit =
    List(
      "truncate table configuration_entries cascade",
      "truncate table package_entries cascade",
      "truncate table parameters cascade",
      "truncate table participant_command_completions cascade",
      "truncate table participant_command_submissions cascade",
      "truncate table participant_events_divulgence cascade",
      "truncate table participant_events_create cascade",
      "truncate table participant_events_consuming_exercise cascade",
      "truncate table participant_events_non_consuming_exercise cascade",
      "truncate table parties cascade",
      "truncate table party_entries cascade",
    ).map(SQL(_)).foreach(_.execute()(connection))

  override def enforceSynchronousCommit(connection: Connection): Unit = () // Not supported

  override def duplicateKeyError: String = "unique constraint"

  val SQL_INSERT_COMMAND: String =
    """merge into participant_command_submissions pcs
      |using dual
      |on (pcs.deduplication_key ={deduplicationKey})
      |when matched then
      |  update set pcs.deduplicate_until={deduplicateUntil}
      |  where pcs.deduplicate_until < {submittedAt}
      |when not matched then
      | insert (pcs.deduplication_key, pcs.deduplicate_until)
      |  values ({deduplicationKey}, {deduplicateUntil})""".stripMargin

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
    OracleSchema.schema.prepareData(dbDtos)

  override def insertBatch(connection: Connection, batch: AppendOnlySchema.Batch): Unit =
    OracleSchema.schema.executeUpdate(batch, connection)

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

  object OracleEventStrategy extends EventStrategy {

    def arrayIntersectionClause(
        columnName: String,
        parties: Set[Ref.Party],
        paramNamePostfix: String,
    ): (String, List[NamedParameter]) =
      (
        s"EXISTS (SELECT 1 FROM JSON_TABLE($columnName, '$$[*]' columns (value PATH '$$')) WHERE value IN ({parties$paramNamePostfix}))",
        List(s"parties$paramNamePostfix" -> parties.map(_.toString)),
      )

    override def filteredEventWitnessesClause(
        witnessesColumnName: String,
        parties: Set[Ref.Party],
    ): (String, List[NamedParameter]) =
      if (parties.size == 1)
        (
          "(json_array({singlePartyfewc}))",
          List[NamedParameter]("singlePartyfewc" -> parties.head.toString),
        )
      else
        (
          s"""(select json_arrayagg(value) from (select value
             |from json_table($witnessesColumnName, '$$[*]' columns (value PATH '$$'))
             |where value IN ({partiesfewc})))
             |""".stripMargin,
          List[NamedParameter]("partiesfewc" -> parties.map(_.toString)),
        )

    override def submittersArePartiesClause(
        submittersColumnName: String,
        parties: Set[Ref.Party],
    ): (String, List[NamedParameter]) = {
      val (clause, params) = arrayIntersectionClause(submittersColumnName, parties, "sapc")
      (s"($clause)", params)
    }

    override def witnessesWhereClause(
        witnessesColumnName: String,
        filterParams: FilterParams,
    ): (String, List[NamedParameter]) = {
      val (wildCardClause, wildCardParams) = filterParams.wildCardParties match {
        case wildCardParties if wildCardParties.isEmpty => (Nil, Nil)
        case wildCardParties =>
          val (clause, params) =
            arrayIntersectionClause(witnessesColumnName, wildCardParties, "wcwwc")
          (
            List(s"($clause)"),
            params,
          )
      }
      val (partiesTemplatesClauses, partiesTemplatesParams) =
        filterParams.partiesAndTemplates.iterator.zipWithIndex
          .map { case ((parties, templateIds), index) =>
            val (clause, params) =
              arrayIntersectionClause(witnessesColumnName, parties, s"ptwwc$index")
            (
              s"( ($clause) AND (template_id IN ({templateIdsArraywwc$index})) )",
              List[NamedParameter](
                s"templateIdsArraywwc$index" -> templateIds.map(_.toString)
              ) ::: params,
            )
          }
          .toList
          .unzip
      (
        (wildCardClause ::: partiesTemplatesClauses).mkString("(", " OR ", ")"),
        wildCardParams ::: partiesTemplatesParams.flatten,
      )
    }

    override def columnEqualityBoolean(column: String, value: String): String =
      s"""case when ($column = $value) then 1 else 0 end"""
  }

  override def eventStrategy: common.EventStrategy = OracleEventStrategy

  // TODO FIXME: confirm this works for oracle
  def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc #${limitClause(Some(1))}"
      .as(get[Long](1).singleOpt)(connection)
  }


  // TODO append-only: remove as part of ContractStorageBackend consolidation, use the data-driven one
  private def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Ref.Party]): String =
    if (parties.isEmpty)
      "false"
    else {
      val NumCharsBetweenParties = 3
      val NumExtraChars = 20
      val OracleMaxStringLiteralLength = 4000

      val groupedParties =
        parties.foldLeft((List.empty[List[String]], 0))({ case ((prev, currentLength), party) =>
          if (
            currentLength + party.length + NumCharsBetweenParties > OracleMaxStringLiteralLength
          ) {
            (List(party) :: prev, party.length + NumExtraChars)
          } else {
            prev match {
              case h :: tail =>
                ((party :: h) :: tail, currentLength + party.length + NumCharsBetweenParties)
              case Nil => (List(party) :: Nil, party.length + NumExtraChars)
            }
          }
        })
      "(" + groupedParties._1
        .map { listOfParties =>
          s"""JSON_EXISTS($arrayColumn, '$$[*]?(@ in ("${listOfParties.mkString("""","""")}"))')"""
        }
        .mkString(" OR ") + ")"
    }

}
