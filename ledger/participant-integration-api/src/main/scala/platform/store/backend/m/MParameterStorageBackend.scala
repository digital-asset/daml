// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.common.MismatchException
import com.daml.platform.store.backend.ParameterStorageBackend

object MParameterStorageBackend extends ParameterStorageBackend {
  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def updateLedgerEnd(ledgerEnd: ParameterStorageBackend.LedgerEnd)(
      connection: Connection
  ): Unit =
    MStore(connection).ledgerEnd = ledgerEnd

  override def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd] =
    Option(MStore(connection).ledgerEnd)

  override def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(
      connection: Connection
  ): Unit = {
    val store = MStore(connection)
    store.synchronized {
      if (
        store.prunedUpToInclusive == null || store.prunedUpToInclusive.toHexString < prunedUpToInclusive.toHexString
      )
        store.prunedUpToInclusive = prunedUpToInclusive
    }
  }

  override def prunedUpToInclusive(connection: Connection): Option[Offset] =
    Option(MStore(connection).prunedUpToInclusive)

  override def updatePrunedAllDivulgedContractsUpToInclusive(prunedUpToInclusive: Offset)(
      connection: Connection
  ): Unit = {
    val store = MStore(connection)
    store.synchronized {
      if (
        store.allDivulgedContractsPrunedUpToInclusive == null || store.allDivulgedContractsPrunedUpToInclusive.toHexString < prunedUpToInclusive.toHexString
      )
        store.allDivulgedContractsPrunedUpToInclusive = prunedUpToInclusive
    }
  }

  override def participantAllDivulgedContractsPrunedUpToInclusive(
      connection: Connection
  ): Option[Offset] =
    Option(MStore(connection).allDivulgedContractsPrunedUpToInclusive)

  override def initializeParameters(
      params: ParameterStorageBackend.IdentityParams
  )(connection: Connection)(implicit loggingContext: LoggingContext): Unit = {
    val previous = ledgerIdentity(connection)
    val ledgerId = params.ledgerId
    val participantId = params.participantId
    previous match {
      case None =>
        logger.info(
          s"Initializing new database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
        )
        MStore(connection).identityParams = params

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
    Option(MStore(connection).identityParams)
}
