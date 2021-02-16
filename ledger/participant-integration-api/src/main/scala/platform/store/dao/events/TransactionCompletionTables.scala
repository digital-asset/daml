package com.daml.platform.store.dao.events

import java.sql.{Connection, PreparedStatement}

// Revisit this one!!
private[events] object TransactionCompletionTables {
  final case class Executables(
      insertCompletions: Connection => PreparedStatement,
      updateLedgerEnd: Connection => PreparedStatement,
  ) {
    def execute(implicit connection: Connection): Unit = {
      updateLedgerEnd(connection).execute()
      val _ = insertCompletions(connection).executeBatch()
    }
  }

  def toExecutables(
      transactionEntries: Seq[TransactionEntry]
  ): TransactionCompletionTables.Executables =
    Executables(
      insertCompletions = (conn: Connection) => {
        transactionEntries.foldLeft(conn.prepareStatement(completionsQuery)) {
          case (preparedStatement, preparedRawEntry) =>
            val maybeSubmitterInfo = preparedRawEntry.submitterInfo
            val offset = preparedRawEntry.offset
            val transactionId = preparedRawEntry.transactionId
            val recordTime = preparedRawEntry.ledgerEffectiveTime
            maybeSubmitterInfo
              .map { submitterInfo =>
                preparedStatement.setObject(1, Array(offset.toByteArray))
                preparedStatement.setArray(
                  2,
                  conn.createArrayOf("TIMESTAMP", Array(java.sql.Timestamp.from(recordTime))),
                )
                preparedStatement.setObject(3, Array[String](submitterInfo.applicationId))
                preparedStatement
                  .setObject(4, Array[String](submitterInfo.actAs.toArray[String].mkString("|")))
                preparedStatement.setObject(5, Array[String](submitterInfo.commandId))
                preparedStatement.setObject(6, Array[String](transactionId))
              }
              .getOrElse {
                preparedStatement.setObject(1, Array.empty[Array[Byte]])
                preparedStatement.setArray(2, conn.createArrayOf("TIMESTAMP", Array.empty[AnyRef]))
                preparedStatement.setObject(3, Array.empty[String])
                preparedStatement.setArray(4, conn.createArrayOf("ARRAY", Array.empty[AnyRef]))
                preparedStatement.setObject(5, Array.empty[String])
                preparedStatement.setObject(6, Array.empty[String])
              }
            preparedStatement.addBatch()
            preparedStatement
        }
      },
      updateLedgerEnd = (conn: Connection) => {
        val preparedStatement = conn.prepareStatement(ledgerEndUpdateQuery)
        // Offset step update!!
        val latestOffset = transactionEntries.maxBy(_.offset).offset.toByteArray
        preparedStatement.setBytes(1, latestOffset)
        preparedStatement.setBytes(2, latestOffset)
        preparedStatement
      },
    )

  private val completionsQuery =
    """insert into participant_command_completions(completion_offset, record_time, application_id, submitters, command_id, transaction_id)
         select completion_offset, record_time, application_id, string_to_array(submitters,'|'), command_id, transaction_id
         from unnest(?, ?, ?, ?, ?, ?) as t(completion_offset, record_time, application_id, submitters, command_id, transaction_id);
    """

  private val ledgerEndUpdateQuery =
    "update parameters set ledger_end = ? where (ledger_end is null or ledger_end < ?);"
}
