// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.backend.{DBDTOV1, StorageBackend}

import scala.collection.mutable

object PostgresStorageBackend extends StorageBackend[RawDBBatchPostgreSQLV1] {

  private val preparedInsertEventsBatchDivulgence: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |INSERT INTO participant_events_divulgence
      | (
      |   event_offset,
      |   contract_id,
      |   command_id,
      |   workflow_id,
      |   application_id,
      |   submitters,
      |   create_argument,
      |   template_id,
      |   tree_event_witnesses,
      |   event_sequential_id,
      |   create_argument_compression
      | )
      | SELECT
      |   event_offset_in,
      |   contract_id_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   string_to_array(submitters_in, '|'),
      |   create_argument_in,
      |   template_id_in,
      |   string_to_array(tree_event_witnesses_in, '|'),
      |   event_sequential_id_in,
      |   create_argument_compression_in::smallint
      | FROM unnest(?,?,?,?,?,?,?,?,?,?,?)
      | as t(
      |   event_offset_in,
      |   contract_id_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   submitters_in,
      |   create_argument_in,
      |   template_id_in,
      |   tree_event_witnesses_in,
      |   event_sequential_id_in,
      |   create_argument_compression_in
      | );
      |
      |""".stripMargin
    )

  private val preparedInsertEventsBatchCreate: Connection => PreparedStatement = _.prepareStatement(
    """
      |INSERT INTO participant_events_create
      | (
      |   event_id,
      |   event_offset,
      |   contract_id,
      |   transaction_id,
      |   ledger_effective_time,
      |   node_index,
      |   command_id,
      |   workflow_id,
      |   application_id,
      |   submitters,
      |   create_argument,
      |   create_signatories,
      |   create_observers,
      |   create_agreement_text,
      |   create_key_value,
      |   create_key_hash,
      |   template_id,
      |   flat_event_witnesses,
      |   tree_event_witnesses,
      |   event_sequential_id,
      |   create_argument_compression,
      |   create_key_value_compression
      | )
      | SELECT
      |   event_id_in,
      |   event_offset_in,
      |   contract_id_in,
      |   transaction_id_in,
      |   ledger_effective_time_in::timestamp,
      |   node_index_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   string_to_array(submitters_in, '|'),
      |   create_argument_in,
      |   string_to_array(create_signatories_in, '|'),
      |   string_to_array(create_observers_in, '|'),
      |   create_agreement_text_in,
      |   create_key_value_in,
      |   create_key_hash_in,
      |   template_id_in,
      |   string_to_array(flat_event_witnesses_in, '|'),
      |   string_to_array(tree_event_witnesses_in, '|'),
      |   event_sequential_id_in,
      |   create_argument_compression_in::smallint,
      |   create_key_value_compression_in::smallint
      | FROM unnest(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      | as t(
      |   event_id_in,
      |   event_offset_in,
      |   contract_id_in,
      |   transaction_id_in,
      |   ledger_effective_time_in,
      |   node_index_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   submitters_in,
      |   create_argument_in,
      |   create_signatories_in,
      |   create_observers_in,
      |   create_agreement_text_in,
      |   create_key_value_in,
      |   create_key_hash_in,
      |   template_id_in,
      |   flat_event_witnesses_in,
      |   tree_event_witnesses_in,
      |   event_sequential_id_in,
      |   create_argument_compression_in,
      |   create_key_value_compression_in
      | );
      |
      |""".stripMargin
  )

  private val preparedInsertEventsBatchConsumingExercise: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |INSERT INTO participant_events_consuming_exercise
      | (
      |   event_id,
      |   event_offset,
      |   contract_id,
      |   transaction_id,
      |   ledger_effective_time,
      |   node_index,
      |   command_id,
      |   workflow_id,
      |   application_id,
      |   submitters,
      |   create_key_value,
      |   exercise_choice,
      |   exercise_argument,
      |   exercise_result,
      |   exercise_actors,
      |   exercise_child_event_ids,
      |   template_id,
      |   flat_event_witnesses,
      |   tree_event_witnesses,
      |   event_sequential_id,
      |   create_key_value_compression,
      |   exercise_argument_compression,
      |   exercise_result_compression
      | )
      | SELECT
      |   event_id_in,
      |   event_offset_in,
      |   contract_id_in,
      |   transaction_id_in,
      |   ledger_effective_time_in::timestamp,
      |   node_index_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   string_to_array(submitters_in, '|'),
      |   create_key_value_in,
      |   exercise_choice_in,
      |   exercise_argument_in,
      |   exercise_result_in,
      |   string_to_array(exercise_actors_in, '|'),
      |   string_to_array(exercise_child_event_ids_in, '|'),
      |   template_id_in,
      |   string_to_array(flat_event_witnesses_in, '|'),
      |   string_to_array(tree_event_witnesses_in, '|'),
      |   event_sequential_id_in,
      |   create_key_value_compression_in::smallint,
      |   exercise_argument_compression_in::smallint,
      |   exercise_result_compression_in::smallint
      | FROM unnest(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      | as t(
      |   event_id_in,
      |   event_offset_in,
      |   contract_id_in,
      |   transaction_id_in,
      |   ledger_effective_time_in,
      |   node_index_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   submitters_in,
      |   create_key_value_in,
      |   exercise_choice_in,
      |   exercise_argument_in,
      |   exercise_result_in,
      |   exercise_actors_in,
      |   exercise_child_event_ids_in,
      |   template_id_in,
      |   flat_event_witnesses_in,
      |   tree_event_witnesses_in,
      |   event_sequential_id_in,
      |   create_key_value_compression_in,
      |   exercise_argument_compression_in,
      |   exercise_result_compression_in
      | );
      |
      |""".stripMargin
    )

  private val preparedInsertEventsBatchNonConsumingExercise: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |INSERT INTO participant_events_non_consuming_exercise
      | (
      |   event_id,
      |   event_offset,
      |   contract_id,
      |   transaction_id,
      |   ledger_effective_time,
      |   node_index,
      |   command_id,
      |   workflow_id,
      |   application_id,
      |   submitters,
      |   create_key_value,
      |   exercise_choice,
      |   exercise_argument,
      |   exercise_result,
      |   exercise_actors,
      |   exercise_child_event_ids,
      |   template_id,
      |   flat_event_witnesses,
      |   tree_event_witnesses,
      |   event_sequential_id,
      |   create_key_value_compression,
      |   exercise_argument_compression,
      |   exercise_result_compression
      | )
      | SELECT
      |   event_id_in,
      |   event_offset_in,
      |   contract_id_in,
      |   transaction_id_in,
      |   ledger_effective_time_in::timestamp,
      |   node_index_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   string_to_array(submitters_in, '|'),
      |   create_key_value_in,
      |   exercise_choice_in,
      |   exercise_argument_in,
      |   exercise_result_in,
      |   string_to_array(exercise_actors_in, '|'),
      |   string_to_array(exercise_child_event_ids_in, '|'),
      |   template_id_in,
      |   string_to_array(flat_event_witnesses_in, '|'),
      |   string_to_array(tree_event_witnesses_in, '|'),
      |   event_sequential_id_in,
      |   create_key_value_compression_in::smallint,
      |   exercise_argument_compression_in::smallint,
      |   exercise_result_compression_in::smallint
      | FROM unnest(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      | as t(
      |   event_id_in,
      |   event_offset_in,
      |   contract_id_in,
      |   transaction_id_in,
      |   ledger_effective_time_in,
      |   node_index_in,
      |   command_id_in,
      |   workflow_id_in,
      |   application_id_in,
      |   submitters_in,
      |   create_key_value_in,
      |   exercise_choice_in,
      |   exercise_argument_in,
      |   exercise_result_in,
      |   exercise_actors_in,
      |   exercise_child_event_ids_in,
      |   template_id_in,
      |   flat_event_witnesses_in,
      |   tree_event_witnesses_in,
      |   event_sequential_id_in,
      |   create_key_value_compression_in,
      |   exercise_argument_compression_in,
      |   exercise_result_compression_in
      | );
      |
      |""".stripMargin
    )

  private val preparedInsertConfigurationEntryBatch: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |INSERT INTO configuration_entries
      | (
      |   ledger_offset,
      |   recorded_at,
      |   submission_id,
      |   typ,
      |   configuration,
      |   rejection_reason
      | )
      | SELECT
      |   ledger_offset_in,
      |   recorded_at_in::timestamp,
      |   submission_id_in,
      |   typ_in,
      |   configuration_in,
      |   rejection_reason_in
      | FROM unnest(?,?,?,?,?,?)
      | as t(
      |   ledger_offset_in,
      |   recorded_at_in,
      |   submission_id_in,
      |   typ_in,
      |   configuration_in,
      |   rejection_reason_in
      | );
      |
      |""".stripMargin
    )

  private val preparedInsertPackageEntryBatch: Connection => PreparedStatement = _.prepareStatement(
    """
      |INSERT INTO package_entries
      | (
      |   ledger_offset,
      |   recorded_at,
      |   submission_id,
      |   typ,
      |   rejection_reason
      | )
      | SELECT
      |   ledger_offset_in,
      |   recorded_at_in::timestamp,
      |   submission_id_in,
      |   typ_in,
      |   rejection_reason_in
      | FROM unnest(?,?,?,?,?)
      | as t(
      |   ledger_offset_in,
      |   recorded_at_in,
      |   submission_id_in,
      |   typ_in,
      |   rejection_reason_in
      | );
      |
      |""".stripMargin
  )

  private val preparedInsertPackageBatch: Connection => PreparedStatement = _.prepareStatement(
    """
      |INSERT INTO packages
      | (
      |   package_id,
      |   upload_id,
      |   source_description,
      |   size,
      |   known_since,
      |   ledger_offset,
      |   package
      | )
      | SELECT
      |   package_id_in,
      |   upload_id_in,
      |   source_description_in,
      |   size_in,
      |   known_since_in::timestamp,
      |   ledger_offset_in,
      |   package_in
      | FROM unnest(?,?,?,?,?,?,?)
      | as t(
      |   package_id_in,
      |   upload_id_in,
      |   source_description_in,
      |   size_in,
      |   known_since_in,
      |   ledger_offset_in,
      |   package_in
      | )
      |  on conflict (package_id) do nothing;
      |
      |""".stripMargin
  )

  private val preparedInsertPartyEntryBatch: Connection => PreparedStatement = _.prepareStatement(
    """
      |INSERT INTO party_entries
      | (
      |   ledger_offset,
      |   recorded_at,
      |   submission_id,
      |   party,
      |   display_name,
      |   typ,
      |   rejection_reason,
      |   is_local
      | )
      | SELECT
      |   ledger_offset_in,
      |   recorded_at_in::timestamp,
      |   submission_id_in,
      |   party_in,
      |   display_name_in,
      |   typ_in,
      |   rejection_reason_in,
      |   is_local_in
      | FROM unnest(?,?,?,?,?,?,?,?)
      | as t(
      |   ledger_offset_in,
      |   recorded_at_in,
      |   submission_id_in,
      |   party_in,
      |   display_name_in,
      |   typ_in,
      |   rejection_reason_in,
      |   is_local_in
      | );
      |
      |""".stripMargin
  )

  private val preparedInsertPartyBatch: Connection => PreparedStatement = _.prepareStatement(
    """
      |INSERT INTO parties
      | (
      |   party,
      |   display_name,
      |   explicit,
      |   ledger_offset,
      |   is_local
      | )
      | SELECT
      |   party_in,
      |   display_name_in,
      |   explicit_in,
      |   ledger_offset_in,
      |   is_local_in
      | FROM unnest(?,?,?,?,?)
      | as t(
      |   party_in,
      |   display_name_in,
      |   explicit_in,
      |   ledger_offset_in,
      |   is_local_in
      | );
      |
      |""".stripMargin
  )

  private val preparedInsertCommandCompletionBatch: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |INSERT INTO participant_command_completions
      | (
      |   completion_offset,
      |   record_time,
      |   application_id,
      |   submitters,
      |   command_id,
      |   transaction_id,
      |   status_code,
      |   status_message
      | )
      | SELECT
      |   completion_offset_in,
      |   record_time_in::timestamp,
      |   application_id_in,
      |   string_to_array(submitters_in, '|'),
      |   command_id_in,
      |   transaction_id_in,
      |   status_code_in,
      |   status_message_in
      | FROM unnest(?,?,?,?,?,?,?,?)
      | as t(
      |   completion_offset_in,
      |   record_time_in,
      |   application_id_in,
      |   submitters_in,
      |   command_id_in,
      |   transaction_id_in,
      |   status_code_in,
      |   status_message_in
      | );
      |
      |""".stripMargin
    )

  private val preparedDeleteCommandSubmissionBatch: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |DELETE FROM participant_command_submissions
      |WHERE deduplication_key IN (
      |  SELECT deduplication_key_in
      |  FROM unnest(?)
      |  as t(deduplication_key_in)
      |)
      |
      |""".stripMargin
    )

  override def insertBatch(
      connection: Connection,
      rawDBBatchPostgreSQLV1: RawDBBatchPostgreSQLV1,
  ): Unit = {
    def execute(preparedStatementFactory: Connection => PreparedStatement, params: Any*): Unit = {
      val preparedStatement = preparedStatementFactory(connection)
      params.view.zipWithIndex.foreach { case (param, zeroBasedIndex) =>
        preparedStatement.setObject(zeroBasedIndex + 1, param)
      }
      preparedStatement.execute()
      ()
    }

    rawDBBatchPostgreSQLV1.commandCompletionsBatch.foreach(batch =>
      execute(
        preparedInsertCommandCompletionBatch,
        batch.completion_offset,
        batch.record_time,
        batch.application_id,
        batch.submitters,
        batch.command_id,
        batch.transaction_id,
        batch.status_code,
        batch.status_message,
      )
    )

    rawDBBatchPostgreSQLV1.configurationEntriesBatch.foreach(batch =>
      execute(
        preparedInsertConfigurationEntryBatch,
        batch.ledger_offset,
        batch.recorded_at,
        batch.submission_id,
        batch.typ,
        batch.configuration,
        batch.rejection_reason,
      )
    )

    rawDBBatchPostgreSQLV1.eventsBatchDivulgence.foreach(batch =>
      execute(
        preparedInsertEventsBatchDivulgence,
        batch.event_offset,
        batch.contract_id,
        batch.command_id,
        batch.workflow_id,
        batch.application_id,
        batch.submitters,
        batch.create_argument,
        batch.template_id,
        batch.tree_event_witnesses,
        batch.event_sequential_id,
        batch.create_argument_compression,
      )
    )

    rawDBBatchPostgreSQLV1.eventsBatchCreate.foreach(batch =>
      execute(
        preparedInsertEventsBatchCreate,
        batch.event_id,
        batch.event_offset,
        batch.contract_id,
        batch.transaction_id,
        batch.ledger_effective_time,
        batch.node_index,
        batch.command_id,
        batch.workflow_id,
        batch.application_id,
        batch.submitters,
        batch.create_argument,
        batch.create_signatories,
        batch.create_observers,
        batch.create_agreement_text,
        batch.create_key_value,
        batch.create_key_hash,
        batch.template_id,
        batch.flat_event_witnesses,
        batch.tree_event_witnesses,
        batch.event_sequential_id,
        batch.create_argument_compression,
        batch.create_key_value_compression,
      )
    )

    rawDBBatchPostgreSQLV1.eventsBatchConsumingExercise.foreach(batch =>
      execute(
        preparedInsertEventsBatchConsumingExercise,
        batch.event_id,
        batch.event_offset,
        batch.contract_id,
        batch.transaction_id,
        batch.ledger_effective_time,
        batch.node_index,
        batch.command_id,
        batch.workflow_id,
        batch.application_id,
        batch.submitters,
        batch.create_key_value,
        batch.exercise_choice,
        batch.exercise_argument,
        batch.exercise_result,
        batch.exercise_actors,
        batch.exercise_child_event_ids,
        batch.template_id,
        batch.flat_event_witnesses,
        batch.tree_event_witnesses,
        batch.event_sequential_id,
        batch.create_key_value_compression,
        batch.exercise_argument_compression,
        batch.exercise_result_compression,
      )
    )

    rawDBBatchPostgreSQLV1.eventsBatchNonConsumingExercise.foreach(batch =>
      execute(
        preparedInsertEventsBatchNonConsumingExercise,
        batch.event_id,
        batch.event_offset,
        batch.contract_id,
        batch.transaction_id,
        batch.ledger_effective_time,
        batch.node_index,
        batch.command_id,
        batch.workflow_id,
        batch.application_id,
        batch.submitters,
        batch.create_key_value,
        batch.exercise_choice,
        batch.exercise_argument,
        batch.exercise_result,
        batch.exercise_actors,
        batch.exercise_child_event_ids,
        batch.template_id,
        batch.flat_event_witnesses,
        batch.tree_event_witnesses,
        batch.event_sequential_id,
        batch.create_key_value_compression,
        batch.exercise_argument_compression,
        batch.exercise_result_compression,
      )
    )

    rawDBBatchPostgreSQLV1.packageEntriesBatch.foreach(batch =>
      execute(
        preparedInsertPackageEntryBatch,
        batch.ledger_offset,
        batch.recorded_at,
        batch.submission_id,
        batch.typ,
        batch.rejection_reason,
      )
    )

    rawDBBatchPostgreSQLV1.packagesBatch.foreach(batch =>
      execute(
        preparedInsertPackageBatch,
        batch.package_id,
        batch.upload_id,
        batch.source_description,
        batch.size,
        batch.known_since,
        batch.ledger_offset,
        batch._package,
      )
    )

    rawDBBatchPostgreSQLV1.partiesBatch.foreach(batch =>
      execute(
        preparedInsertPartyBatch,
        batch.party,
        batch.display_name,
        batch.explicit,
        batch.ledger_offset,
        batch.is_local,
      )
    )

    rawDBBatchPostgreSQLV1.partyEntriesBatch.foreach(batch =>
      execute(
        preparedInsertPartyEntryBatch,
        batch.ledger_offset,
        batch.recorded_at,
        batch.submission_id,
        batch.party,
        batch.display_name,
        batch.typ,
        batch.rejection_reason,
        batch.is_local,
      )
    )

    rawDBBatchPostgreSQLV1.commandDeduplicationBatch.foreach(batch =>
      execute(preparedDeleteCommandSubmissionBatch, batch.deduplication_key)
    )
  }

  private val preparedUpdateLedgerEnd: Connection => PreparedStatement = _.prepareStatement(
    """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = ?,
      |  ledger_end_sequential_id = ?
      |
      |""".stripMargin
  )

  private val preparedUpdateLedgerEndWithConfig: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = ?,
      |  ledger_end_sequential_id = ?,
      |  configuration = ?
      |
      |""".stripMargin
    )

  override def updateParams(connection: Connection, params: StorageBackend.Params): Unit = {
    params.configuration match {
      case Some(configBytes) =>
        // TODO append-only: just a shortcut, proper solution: reading config with a temporal query
        val preparedStatement = preparedUpdateLedgerEndWithConfig(connection)
        preparedStatement.setString(1, params.ledgerEnd.toHexString)
        preparedStatement.setLong(2, params.eventSeqId)
        preparedStatement.setBytes(3, configBytes)
        preparedStatement.execute()

      case None =>
        val preparedStatement = preparedUpdateLedgerEnd(connection)
        preparedStatement.setString(1, params.ledgerEnd.toHexString)
        preparedStatement.setLong(2, params.eventSeqId)
        preparedStatement.execute()

    }
    ()
  }

  override def initialize(connection: Connection): StorageBackend.Initialized = {
    val (offset, eventSeqId) = queryLedgerEndAndEventSeqId(connection)

    // TODO append-only: verify default isolation level is enough to maintain consistency here (eg the fact of selecting these values at the beginning ensures data changes to the params are postponed until purging finishes). Alternatively: if single indexer instance to db access otherwise ensured, atomicity here is not an issue.

    offset.foreach { existingOffset =>
      val preparedStatement = preparedDeleteIngestionOverspillEntries(connection)
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).foreach(
        preparedStatement.setString(_, existingOffset.toHexString)
      )
      preparedStatement.execute()
    }

    StorageBackend.Initialized(
      lastOffset = offset,
      lastEventSeqId = eventSeqId,
    )
  }

  private def queryLedgerEndAndEventSeqId(
      connection: Connection
  ): (Option[Offset], Option[Long]) = {
    val queryStatement = connection.createStatement()
    val params = fetch(
      queryStatement.executeQuery(
        """
        |SELECT
        |  ledger_end,
        |  ledger_end_sequential_id
        |FROM
        |  parameters
        |
        |""".stripMargin
      )
    )(rs =>
      (
        if (rs.getString(1) == null) None
        else Some(Offset.fromHexString(Ref.HexString.assertFromString(rs.getString(1)))),
        Option(rs.getLong(2)),
      )
    )
    queryStatement.close()
    assert(params.size == 1)
    params.head
  }

  private val preparedDeleteIngestionOverspillEntries: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |DELETE
      |FROM configuration_entries
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM package_entries
      |WHERE ledger_offset > ?;
      |
      |-- TODO append-only: we do not have currently an index to support efficiently this operation. either add or make sure it is okay to do full table scans here
      |DELETE
      |FROM packages
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM participant_command_completions
      |WHERE completion_offset > ?;
      |
      |DELETE
      |FROM participant_events_divulgence
      |WHERE event_offset > ?;
      |
      |DELETE
      |FROM participant_events_create
      |WHERE event_offset > ?;
      |
      |DELETE
      |FROM participant_events_consuming_exercise
      |WHERE event_offset > ?;
      |
      |DELETE
      |FROM participant_events_non_consuming_exercise
      |WHERE event_offset > ?;
      |
      |-- TODO append-only: we do not have currently an index to support efficiently this operation. either add or make sure it is okay to do full table scans here
      |DELETE
      |FROM parties
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM party_entries
      |WHERE ledger_offset > ?;
      |
      |""".stripMargin
    )

  private def fetch[T](resultSet: ResultSet)(parse: ResultSet => T): Vector[T] = {
    val buffer = mutable.ArrayBuffer.empty[T]
    while (resultSet.next()) {
      buffer += parse(resultSet)
    }
    resultSet.close()
    buffer.toVector
  }

  override def batch(dbDtos: Vector[DBDTOV1]): RawDBBatchPostgreSQLV1 = {
    val builder = RawDBBatchPostgreSQLV1.Builder()
    dbDtos.foreach(builder.add)
    builder.build()
  }
}
