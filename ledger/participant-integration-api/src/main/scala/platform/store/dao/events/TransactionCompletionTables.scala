package com.daml.platform.store.dao.events

import java.sql.Connection

import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.dao.{CommandCompletionsTable, ParametersTable}

private[events] object TransactionCompletionTables {
  final case class Executables(
      insertCompletions: Connection => Unit,
      updateLedgerEnd: Connection => Unit,
  ) {
    def execute(implicit connection: Connection): Unit = {
      updateLedgerEnd(connection)
      insertCompletions(connection)
    }
  }

  def toExecutables(
      offsetStep: OffsetStep,
      transactionEntries: Seq[TransactionEntry],
  ): TransactionCompletionTables.Executables =
    Executables(
      insertCompletions = (conn: Connection) => {
        transactionEntries.foreach { transactionEntry =>
          val maybeSubmitterInfo = transactionEntry.submitterInfo
          val offset = transactionEntry.offsetStep.offset
          val transactionId = transactionEntry.transactionId
          val recordTime = transactionEntry.ledgerEffectiveTime
          maybeSubmitterInfo
            .map(
              CommandCompletionsTable.prepareCompletionInsert(
                _,
                offset = offset,
                transactionId = transactionId,
                recordTime = recordTime,
              )
            )
            .foreach(_.execute()(conn))
        }
      },
      updateLedgerEnd = ParametersTable.updateLedgerEnd(offsetStep)(_),
    )
}
