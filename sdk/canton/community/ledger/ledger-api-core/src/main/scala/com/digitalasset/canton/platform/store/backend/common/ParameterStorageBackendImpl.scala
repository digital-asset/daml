// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{int, long}
import anorm.{RowParser, ~}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.domain.ParticipantId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.Conversions.offset
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.{Conversions, ParameterStorageBackend}
import com.digitalasset.canton.tracing.TraceContext
import scalaz.syntax.tag.*

import java.sql.Connection

private[backend] class ParameterStorageBackendImpl(queryStrategy: QueryStrategy)
    extends ParameterStorageBackend {

  override def updateLedgerEnd(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    import Conversions.OffsetToStatement
    queryStrategy.forceSynchronousCommitForCurrentTransactionForPostgreSQL(connection)
    discard(
      SQL"""
        UPDATE
          lapi_parameters
        SET
          ledger_end = ${ledgerEnd.lastOffset},
          ledger_end_sequential_id = ${ledgerEnd.lastEventSeqId},
          ledger_end_string_interning_id = ${ledgerEnd.lastStringInterningId}
        """
        .execute()(connection)
    )
  }

  private val SqlGetLedgerEnd =
    SQL"""
      SELECT
        ledger_end,
        ledger_end_sequential_id,
        ledger_end_string_interning_id
      FROM
        lapi_parameters
      """

  override def ledgerEnd(connection: Connection): ParameterStorageBackend.LedgerEnd =
    SqlGetLedgerEnd
      .as(LedgerEndParser.singleOpt)(connection)
      .getOrElse(ParameterStorageBackend.LedgerEnd.beforeBegin)

  private val TableName: String = "lapi_parameters"
  private val ParticipantIdColumnName: String = "participant_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val LedgerEndSequentialIdColumnName: String = "ledger_end_sequential_id"
  private val LedgerEndStringInterningIdColumnName: String = "ledger_end_string_interning_id"

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
    ParticipantIdParser map { case participantId =>
      ParameterStorageBackend.IdentityParams(participantId)
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
      params: ParameterStorageBackend.IdentityParams,
      loggerFactory: NamedLoggerFactory,
  )(connection: Connection): Unit = {
    val logger = loggerFactory.getTracedLogger(getClass)
    implicit val traceContext: TraceContext = TraceContext.empty
    // Note: this method is the only one that inserts a row into the parameters table
    val previous = ledgerIdentity(connection)
    val participantId = params.participantId
    previous match {
      case None =>
        logger.info(
          s"Initializing new database for participantId '${params.participantId}'"
        )
        import Conversions.OffsetToStatement
        val ledgerEnd = ParameterStorageBackend.LedgerEnd.beforeBegin
        discard(
          SQL"""insert into #$TableName(
              #$ParticipantIdColumnName,
              #$LedgerEndColumnName,
              #$LedgerEndSequentialIdColumnName,
              #$LedgerEndStringInterningIdColumnName
            ) values(
              ${participantId.unwrap: String},
              ${ledgerEnd.lastOffset},
              ${ledgerEnd.lastEventSeqId},
              ${ledgerEnd.lastStringInterningId}
            )"""
            .execute()(connection)
        )
      case Some(ParameterStorageBackend.IdentityParams(`participantId`)) =>
        logger.info(
          s"Found existing database for participantId '${params.participantId}'"
        )
      case Some(ParameterStorageBackend.IdentityParams(existing)) =>
        logger.error(
          s"Found existing database with mismatching participantId: existing '$existing', provided '${params.participantId}'"
        )
        throw new MismatchException.ParticipantId(
          existing = existing,
          provided = params.participantId,
        )
    }
  }

  override def ledgerIdentity(
      connection: Connection
  ): Option[ParameterStorageBackend.IdentityParams] =
    SQL"select #$ParticipantIdColumnName from #$TableName"
      .as(LedgerIdentityParser.singleOpt)(connection)

  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit = {
    import Conversions.OffsetToStatement
    discard(
      SQL"""
        update lapi_parameters set participant_pruned_up_to_inclusive=$prunedUpToInclusive
        where participant_pruned_up_to_inclusive < $prunedUpToInclusive or participant_pruned_up_to_inclusive is null
        """
        .execute()(connection)
    )
  }

  def updatePrunedAllDivulgedContractsUpToInclusive(
      prunedUpToInclusive: Offset
  )(connection: Connection): Unit = {
    import Conversions.OffsetToStatement
    discard(
      SQL"""
        update lapi_parameters set participant_all_divulged_contracts_pruned_up_to_inclusive=$prunedUpToInclusive
        where participant_all_divulged_contracts_pruned_up_to_inclusive < $prunedUpToInclusive or participant_all_divulged_contracts_pruned_up_to_inclusive is null
        """
        .execute()(connection)
    )
  }

  private val SqlSelectMostRecentPruning =
    SQL"select participant_pruned_up_to_inclusive from lapi_parameters"

  def prunedUpToInclusive(connection: Connection): Option[Offset] =
    SqlSelectMostRecentPruning
      .as(offset("participant_pruned_up_to_inclusive").?.single)(connection)

  private val SqlSelectMostRecentPruningAllDivulgedContracts =
    SQL"select participant_all_divulged_contracts_pruned_up_to_inclusive from lapi_parameters"

  def participantAllDivulgedContractsPrunedUpToInclusive(
      connection: Connection
  ): Option[Offset] = {
    SqlSelectMostRecentPruningAllDivulgedContracts
      .as(offset("participant_all_divulged_contracts_pruned_up_to_inclusive").?.single)(
        connection
      )
  }

  private val SqlSelectMostRecentPruningAndLedgerEnd =
    SQL"select participant_pruned_up_to_inclusive, #$LedgerEndColumnName from lapi_parameters"

  private val PruneUptoInclusiveAndLedgerEndParser
      : RowParser[ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd] =
    offset("participant_pruned_up_to_inclusive").? ~ LedgerEndOffsetParser map {
      case pruneUptoInclusive ~ ledgerEndOffset =>
        ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd(
          pruneUptoInclusive = pruneUptoInclusive,
          ledgerEnd = ledgerEndOffset,
        )
    }

  override def prunedUpToInclusiveAndLedgerEnd(
      connection: Connection
  ): ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd =
    SqlSelectMostRecentPruningAndLedgerEnd
      .as(PruneUptoInclusiveAndLedgerEndParser.singleOpt)(connection)
      .getOrElse(
        ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd(
          pruneUptoInclusive = None,
          ledgerEnd = Offset.beforeBegin,
        )
      )
}
