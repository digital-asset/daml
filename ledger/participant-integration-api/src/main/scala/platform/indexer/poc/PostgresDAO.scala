// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.poc

import java.sql.{DriverManager, PreparedStatement, ResultSet}

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait PostgresDAO {
  def insertBatch(batch: RawDBBatchPostgreSQLV1): Unit

  def updateParams(ledgerEnd: Offset, eventSeqId: Long, configuration: Option[Array[Byte]]): Unit

  def initialize: (Option[Offset], Option[Long])
}

case class JDBCPostgresDAO(jdbcUrl: String) extends PostgresDAO with AutoCloseable {

  private val connection = {
    val c = DriverManager.getConnection(jdbcUrl)
    c.setAutoCommit(false)

    val stmnt = c.createStatement()
    stmnt.execute("SET synchronous_commit TO OFF;")
    c.commit()
    stmnt.close()

    c
  }

  private val preparedInsertEventsBatch = connection.prepareStatement(
    """
      |INSERT INTO participant_events
      | (
      |   event_kind,
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
      |   exercise_choice,
      |   exercise_argument,
      |   exercise_result,
      |   exercise_actors,
      |   exercise_child_event_ids,
      |   template_id,
      |   flat_event_witnesses,
      |   tree_event_witnesses,
      |   event_sequential_id,
      |   create_argument_compression,
      |   create_key_value_compression,
      |   exercise_argument_compression,
      |   exercise_result_compression
      | )
      | SELECT
      |   event_kind_in,
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
      |   exercise_choice_in,
      |   exercise_argument_in,
      |   exercise_result_in,
      |   string_to_array(exercise_actors_in, '|'),
      |   string_to_array(exercise_child_event_ids_in, '|'),
      |   template_id_in,
      |   string_to_array(flat_event_witnesses_in, '|'),
      |   string_to_array(tree_event_witnesses_in, '|'),
      |   event_sequential_id_in,
      |   create_argument_compression_in::smallint,
      |   create_key_value_compression_in::smallint,
      |   exercise_argument_compression_in::smallint,
      |   exercise_result_compression_in::smallint
      | FROM unnest(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      | as t(
      |   event_kind_in,
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
      |   exercise_choice_in,
      |   exercise_argument_in,
      |   exercise_result_in,
      |   exercise_actors_in,
      |   exercise_child_event_ids_in,
      |   template_id_in,
      |   flat_event_witnesses_in,
      |   tree_event_witnesses_in,
      |   event_sequential_id_in,
      |   create_argument_compression_in,
      |   create_key_value_compression_in,
      |   exercise_argument_compression_in,
      |   exercise_result_compression_in
      | );
      |
      |""".stripMargin
  )

  private val preparedInsertConfigurationEntryBatch = connection.prepareStatement(
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

  private val preparedInsertPackageEntryBatch = connection.prepareStatement(
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

  private val preparedInsertPackageBatch = connection.prepareStatement(
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
      | );
      |
      |""".stripMargin
  )

  private val preparedInsertPartyEntryBatch = connection.prepareStatement(
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

  private val preparedInsertPartyBatch: PreparedStatement = connection.prepareStatement(
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

  private val preparedInsertCommandCompletionBatch = connection.prepareStatement(
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

  private val preparedDeleteCommandSubmissionBatch = connection.prepareStatement(
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

  override def insertBatch(rawDBBatchPostgreSQLV1: RawDBBatchPostgreSQLV1): Unit = {
    def execute(preparedStatement: PreparedStatement, params: Any*): Unit = {
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

    rawDBBatchPostgreSQLV1.eventsBatch.foreach(batch =>
      execute(
        preparedInsertEventsBatch,
        batch.event_kind,
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
        batch.exercise_choice,
        batch.exercise_argument,
        batch.exercise_result,
        batch.exercise_actors,
        batch.exercise_child_event_ids,
        batch.template_id,
        batch.flat_event_witnesses,
        batch.tree_event_witnesses,
        batch.event_sequential_id,
        batch.create_argument_compression,
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

    connection.commit()
  }

  private val preparedUpdateLedgerEnd = connection.prepareStatement(
    """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = ?,
      |  ledger_end_sequential_id = ?
      |
      |""".stripMargin
  )

  private val preparedUpdateLedgerEndWithConfig = connection.prepareStatement(
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

  override def updateParams(
      ledgerEnd: Offset,
      eventSeqId: Long,
      configuration: Option[Array[Byte]],
  ): Unit = {
    configuration match {
      case Some(configBytes) =>
        // TODO just a shortcut, proper solution: reading config with a temporal query
        preparedUpdateLedgerEndWithConfig.setObject(1, ledgerEnd.toByteArray)
        preparedUpdateLedgerEndWithConfig.setLong(2, eventSeqId)
        preparedUpdateLedgerEndWithConfig.setBytes(3, configBytes)
        preparedUpdateLedgerEndWithConfig.execute()

      case None =>
        preparedUpdateLedgerEnd.setObject(1, ledgerEnd.toByteArray)
        preparedUpdateLedgerEnd.setLong(2, eventSeqId)
        preparedUpdateLedgerEnd.execute()

    }
    connection.commit()
  }

  override def initialize: (Option[Offset], Option[Long]) = {
    val result @ (offset, _) = queryLedgerAndAndEventSeqId()

    // TODO verify default isolation level is enough to maintain consistency here (eg the fact of selecting these values at the beginning ensures data changes to the params are postponed until purging finishes). Alternatively: if single indexer instance to db access otherwise ensured, atomicity here is not an issue.

    offset.foreach { existingOffset =>
      List(1, 2, 3, 4, 5, 6, 7).foreach(
        preparedDeleteIngestionOverspillEntries.setBytes(_, existingOffset.toByteArray)
      )
      preparedDeleteIngestionOverspillEntries.execute()
    }

    connection.commit()

    result
  }

  private def queryLedgerAndAndEventSeqId(): (Option[Offset], Option[Long]) = {
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
        if (rs.getBytes(1) == null) None else Some(Offset.fromByteArray(rs.getBytes(1))),
        Option(rs.getLong(2)),
      )
    )
    queryStatement.close()
    assert(params.size == 1)
    params.head
  }

  private val preparedDeleteIngestionOverspillEntries: PreparedStatement =
    connection.prepareStatement(
      """
      |DELETE
      |FROM configuration_entries
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM package_entries
      |WHERE ledger_offset > ?;
      |
      |-- TODO we do not have currently an index to support efficiently this operation. either add or make sure it is okay to do full table scans here
      |DELETE
      |FROM packages
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM participant_command_completions
      |WHERE completion_offset > ?;
      |
      |DELETE
      |FROM participant_events
      |WHERE event_offset > ?;
      |
      |-- TODO we do not have currently an index to support efficiently this operation. either add or make sure it is okay to do full table scans here
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

  override def close(): Unit = connection.close()

}

object PostgresDAO {

  case class ResourcePool[T <: AutoCloseable](create: () => T, size: Int) extends AutoCloseable {
    assert(size > 0)

    private val pool = mutable.Queue.empty[T]
    pool ++= List.fill(size)(create())

    def borrow[U](block: T => U): U = {
      val resource = synchronized {
        if (pool.isEmpty) None
        else Some(pool.dequeue())
      } match {
        case None => throw new Exception("non-growing pool exhausted, please tune pool size")
        case Some(resource) => resource
      }

      Try(block(resource)) match {
        case Success(result) =>
          synchronized {
            pool.enqueue(resource)
          }
          result

        case Failure(exception) =>
          synchronized {
            pool.enqueue(create())
          }
          Try(resource.close())
          throw exception
      }
    }

    override def close(): Unit =
      pool.foreach(_.close())
  }

}
