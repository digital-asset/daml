// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.SqlParser.{int, long}
import anorm.{RowParser, ~}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.common.MismatchException
import com.daml.platform.store.backend.Conversions.{ledgerString, offset}
import com.daml.platform.store.backend.{Conversions, ParameterStorageBackend}
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.scalautil.Statement.discard
import scalaz.syntax.tag._

import java.sql.Connection

private[backend] object ParameterStorageBackendImpl extends ParameterStorageBackend {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def updateLedgerEnd(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    import Conversions.OffsetToStatement
    SQL"""
      UPDATE
        parameters
      SET
        ledger_end = ${ledgerEnd.lastOffset},
        ledger_end_sequential_id = ${ledgerEnd.lastEventSeqId},
        ledger_end_string_interning_id = ${ledgerEnd.lastStringInterningId}
      """
      .execute()(connection)
    ()
  }

  private val SqlGetLedgerEnd =
    SQL"""
      SELECT
        ledger_end,
        ledger_end_sequential_id,
        ledger_end_string_interning_id
      FROM
        parameters
      """

  override def ledgerEnd(connection: Connection): ParameterStorageBackend.LedgerEnd =
    SqlGetLedgerEnd
      .as(LedgerEndParser.singleOpt)(connection)
      .getOrElse(ParameterStorageBackend.LedgerEnd.beforeBegin)

  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val ParticipantIdColumnName: String = "participant_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val LedgerEndSequentialIdColumnName: String = "ledger_end_sequential_id"
  private val LedgerEndStringInterningIdColumnName: String = "ledger_end_string_interning_id"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val ParticipantIdParser: RowParser[ParticipantId] =
    Conversions.participantId(ParticipantIdColumnName).map(ParticipantId(_))

  private val LedgerEndOffsetParser: RowParser[Offset] = {
    // Note: the ledger_end is a non-optional column,
    // however some databases treat Offset.beforeBegin (the empty string) as NULL
    offset(LedgerEndColumnName).?.map(_.getOrElse(Offset.beforeBegin))
  }

  private val LedgerEndSequentialIdParser: RowParser[Long] =
    long(LedgerEndSequentialIdColumnName)

  private val LedgerEndStringInterningIdParser: RowParser[Int] =
    int(LedgerEndStringInterningIdColumnName)

  private val LedgerIdentityParser: RowParser[ParameterStorageBackend.IdentityParams] =
    LedgerIdParser ~ ParticipantIdParser map { case ledgerId ~ participantId =>
      ParameterStorageBackend.IdentityParams(ledgerId, participantId)
    }

  private val LedgerEndParser: RowParser[ParameterStorageBackend.LedgerEnd] =
    LedgerEndOffsetParser ~ LedgerEndSequentialIdParser ~ LedgerEndStringInterningIdParser map {
      case lastOffset ~ lastEventSequentialId ~ lastStringInterningId =>
        ParameterStorageBackend.LedgerEnd(
          lastOffset,
          lastEventSequentialId,
          lastStringInterningId,
        )
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
        import Conversions.OffsetToStatement
        val ledgerEnd = ParameterStorageBackend.LedgerEnd.beforeBegin
        discard(
          SQL"""insert into #$TableName(
              #$LedgerIdColumnName,
              #$ParticipantIdColumnName,
              #$LedgerEndColumnName,
              #$LedgerEndSequentialIdColumnName,
              #$LedgerEndStringInterningIdColumnName
            ) values(
              ${ledgerId.unwrap},
              ${participantId.unwrap: String},
              ${ledgerEnd.lastOffset},
              ${ledgerEnd.lastEventSeqId},
              ${ledgerEnd.lastStringInterningId}
            )"""
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

  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit = {
    import Conversions.OffsetToStatement
    SQL"""
      update parameters set participant_pruned_up_to_inclusive=$prunedUpToInclusive
      where participant_pruned_up_to_inclusive < $prunedUpToInclusive or participant_pruned_up_to_inclusive is null
      """
      .execute()(connection)
    ()
  }

  def updatePrunedAllDivulgedContractsUpToInclusive(
      prunedUpToInclusive: Offset
  )(connection: Connection): Unit = {
    import Conversions.OffsetToStatement
    SQL"""
      update parameters set participant_all_divulged_contracts_pruned_up_to_inclusive=$prunedUpToInclusive
      where participant_all_divulged_contracts_pruned_up_to_inclusive < $prunedUpToInclusive or participant_all_divulged_contracts_pruned_up_to_inclusive is null
      """
      .execute()(connection)
    ()
  }

  private val SqlSelectMostRecentPruning =
    SQL"select participant_pruned_up_to_inclusive from parameters"

  def prunedUpToInclusive(connection: Connection): Option[Offset] =
    SqlSelectMostRecentPruning
      .as(offset("participant_pruned_up_to_inclusive").?.single)(connection)

  private val SqlSelectMostRecentPruningAllDivulgedContracts =
    SQL"select participant_all_divulged_contracts_pruned_up_to_inclusive from parameters"

  def participantAllDivulgedContractsPrunedUpToInclusive(
      connection: Connection
  ): Option[Offset] = {
    SqlSelectMostRecentPruningAllDivulgedContracts
      .as(offset("participant_all_divulged_contracts_pruned_up_to_inclusive").?.single)(
        connection
      )
  }

}
