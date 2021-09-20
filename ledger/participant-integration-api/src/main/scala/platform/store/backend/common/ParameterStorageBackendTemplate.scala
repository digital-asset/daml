// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.long
import anorm.{RowParser, SQL, ~}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.Conversions.{ledgerString, offset}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.common.MismatchException
import com.daml.platform.store.Conversions
import com.daml.platform.store.backend.ParameterStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.scalautil.Statement.discard
import scalaz.syntax.tag._

private[backend] trait ParameterStorageBackendTemplate extends ParameterStorageBackend {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private val SQL_UPDATE_LEDGER_END = SQL(
    """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = {ledger_end},
      |  ledger_end_sequential_id = {ledger_end_sequential_id}
      |""".stripMargin
  )

  override def updateLedgerEnd(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_UPDATE_LEDGER_END
      .on("ledger_end" -> ledgerEnd.lastOffset)
      .on("ledger_end_sequential_id" -> ledgerEnd.lastEventSeqId)
      .execute()(connection)
    ()
  }

  private val SQL_GET_LEDGER_END = SQL(
    """
      |SELECT
      |  ledger_end,
      |  ledger_end_sequential_id
      |FROM
      |  parameters
      |
      |""".stripMargin
  )

  override def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd] =
    SQL_GET_LEDGER_END.as(LedgerEndParser.singleOpt)(connection).flatten

  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val ParticipantIdColumnName: String = "participant_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val LedgerEndSequentialIdColumnName: String = "ledger_end_sequential_id"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val ParticipantIdParser: RowParser[ParticipantId] =
    Conversions.participantId(ParticipantIdColumnName).map(ParticipantId(_))

  private val LedgerEndOffsetParser: RowParser[Option[Offset]] =
    offset(LedgerEndColumnName).?

  private val LedgerEndSequentialIdParser: RowParser[Option[Long]] =
    long(LedgerEndSequentialIdColumnName).?

  private val LedgerIdentityParser: RowParser[ParameterStorageBackend.IdentityParams] =
    LedgerIdParser ~ ParticipantIdParser map { case ledgerId ~ participantId =>
      ParameterStorageBackend.IdentityParams(ledgerId, participantId)
    }

  private val LedgerEndParser: RowParser[Option[ParameterStorageBackend.LedgerEnd]] =
    LedgerEndOffsetParser ~ LedgerEndSequentialIdParser map {
      case Some(lastOffset) ~ Some(lastEventSequentialId) =>
        Some(ParameterStorageBackend.LedgerEnd(lastOffset, lastEventSequentialId))
      case _ =>
        // Note: offset and event sequential id are always written together.
        // Cases where only one of them is defined are not handled here.
        None
    }

  override def initializeParameters(
      params: ParameterStorageBackend.IdentityParams
  )(connection: Connection)(implicit loggingContext: LoggingContext): Unit = {
    // Note: this method is the only one that inserts a row into the parameters table
    val previous = ledgerIdentity(connection)
    val ledgerId = params.ledgerId
    val participantId = params.participantId
    previous match {
      case None =>
        logger.info(
          s"Initializing new database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
        discard(
          SQL"insert into #$TableName(#$LedgerIdColumnName, #$ParticipantIdColumnName) values(${ledgerId.unwrap}, ${participantId.unwrap: String})"
            .execute()(connection)
        )
      case Some(ParameterStorageBackend.IdentityParams(`ledgerId`, `participantId`)) =>
        logger.info(
          s"Found existing database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
      case Some(ParameterStorageBackend.IdentityParams(existing, _))
          if existing != params.ledgerId =>
        logger.error(
          s"Found existing database with mismatching ledgerId: existing '$existing', provided '${params.ledgerId}'"
        )
        throw MismatchException.LedgerId(
          existing = existing,
          provided = params.ledgerId,
        )
      case Some(ParameterStorageBackend.IdentityParams(_, existing)) =>
        logger.error(
          s"Found existing database with mismatching participantId: existing '$existing', provided '${params.participantId}'"
        )
        throw MismatchException.ParticipantId(
          existing = existing,
          provided = params.participantId,
        )
    }
  }

  override def ledgerIdentity(
      connection: Connection
  ): Option[ParameterStorageBackend.IdentityParams] =
    SQL"select #$LedgerIdColumnName, #$ParticipantIdColumnName from #$TableName"
      .as(LedgerIdentityParser.singleOpt)(connection)

  private val SQL_UPDATE_MOST_RECENT_PRUNING =
    SQL("""
          |update parameters set participant_pruned_up_to_inclusive={pruned_up_to_inclusive}
          |where participant_pruned_up_to_inclusive < {pruned_up_to_inclusive} or participant_pruned_up_to_inclusive is null
          |""".stripMargin)

  private val SQL_UPDATE_MOST_RECENT_PRUNING_INCLUDING_ALL_DIVULGED_CONTRACTS =
    SQL("""
          |update parameters set participant_all_divulged_contracts_pruned_up_to_inclusive={prune_all_divulged_contracts_up_to_inclusive}
          |where participant_pruned_up_to_inclusive < {prune_all_divulged_contracts_up_to_inclusive} or participant_all_divulged_contracts_pruned_up_to_inclusive is null
          |""".stripMargin)

  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_UPDATE_MOST_RECENT_PRUNING
      .on("pruned_up_to_inclusive" -> prunedUpToInclusive)
      .execute()(connection)
    ()
  }

  def updatePrunedAllDivulgedContractsUpToInclusive(
      prunedUpToInclusive: Offset
  )(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    SQL_UPDATE_MOST_RECENT_PRUNING_INCLUDING_ALL_DIVULGED_CONTRACTS
      .on("prune_all_divulged_contracts_up_to_inclusive" -> prunedUpToInclusive)
      .execute()(connection)
    ()
  }

  private val SQL_SELECT_MOST_RECENT_PRUNING = SQL(
    "select participant_pruned_up_to_inclusive from parameters"
  )

  def prunedUptoInclusive(connection: Connection): Option[Offset] =
    SQL_SELECT_MOST_RECENT_PRUNING
      .as(offset("participant_pruned_up_to_inclusive").?.single)(connection)
}
