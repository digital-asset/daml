// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{int, long}
import anorm.{RowParser, ~}
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

private[backend] object ParameterStorageBackendTemplate extends ParameterStorageBackend {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def updateLedgerEnd(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
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

  override def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd] =
    SqlGetLedgerEnd.as(LedgerEndParser.singleOpt)(connection).flatten

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

  private val LedgerEndOffsetParser: RowParser[Option[Offset]] =
    offset(LedgerEndColumnName).?

  private val LedgerEndSequentialIdParser: RowParser[Option[Long]] =
    long(LedgerEndSequentialIdColumnName).?

  private val LedgerEndStringInterningIdParser: RowParser[Option[Int]] =
    int(LedgerEndStringInterningIdColumnName).?

  private val LedgerIdentityParser: RowParser[ParameterStorageBackend.IdentityParams] =
    LedgerIdParser ~ ParticipantIdParser map { case ledgerId ~ participantId =>
      ParameterStorageBackend.IdentityParams(ledgerId, participantId)
    }

  private val LedgerEndParser: RowParser[Option[ParameterStorageBackend.LedgerEnd]] =
    LedgerEndOffsetParser ~ LedgerEndSequentialIdParser ~ LedgerEndStringInterningIdParser map {
      case Some(lastOffset) ~ Some(lastEventSequentialId) ~ Some(lastStringInterningId) =>
        Some(
          ParameterStorageBackend.LedgerEnd(
            lastOffset,
            lastEventSequentialId,
            lastStringInterningId,
          )
        )
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

  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""
      update parameters set participant_pruned_up_to_inclusive=${prunedUpToInclusive}
      where participant_pruned_up_to_inclusive < ${prunedUpToInclusive} or participant_pruned_up_to_inclusive is null
      """
      .execute()(connection)
    ()
  }

  def updatePrunedAllDivulgedContractsUpToInclusive(
      prunedUpToInclusive: Offset
  )(connection: Connection): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""
      update parameters set participant_all_divulged_contracts_pruned_up_to_inclusive=${prunedUpToInclusive}
      where participant_all_divulged_contracts_pruned_up_to_inclusive < ${prunedUpToInclusive} or participant_all_divulged_contracts_pruned_up_to_inclusive is null
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
