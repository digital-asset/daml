// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    MStore.update(connection)(_.copy(ledgerEnd = ledgerEnd))

  override def ledgerEnd(connection: Connection): ParameterStorageBackend.LedgerEnd =
    Option(MStore(connection)(_.ledgerEnd)).getOrElse(ParameterStorageBackend.LedgerEnd.beforeBegin)

  override def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(
      connection: Connection
  ): Unit = MStore.update(connection) { mStore =>
    if (
      mStore.prunedUpToInclusive == null ||
      mStore.prunedUpToInclusive.toHexString < prunedUpToInclusive.toHexString
    ) mStore.copy(prunedUpToInclusive = prunedUpToInclusive)
    else mStore
  }

  override def prunedUpToInclusive(connection: Connection): Option[Offset] =
    Option(MStore(connection)(_.prunedUpToInclusive))

  override def updatePrunedAllDivulgedContractsUpToInclusive(prunedUpToInclusive: Offset)(
      connection: Connection
  ): Unit = MStore.update(connection) { mStore =>
    if (
      mStore.allDivulgedContractsPrunedUpToInclusive == null ||
      mStore.allDivulgedContractsPrunedUpToInclusive.toHexString < prunedUpToInclusive.toHexString
    ) mStore.copy(allDivulgedContractsPrunedUpToInclusive = prunedUpToInclusive)
    else mStore
  }

  override def participantAllDivulgedContractsPrunedUpToInclusive(
      connection: Connection
  ): Option[Offset] =
    Option(MStore(connection)(_.allDivulgedContractsPrunedUpToInclusive))

  override def initializeParameters(
      params: ParameterStorageBackend.IdentityParams
  )(connection: Connection)(implicit loggingContext: LoggingContext): Unit =
    MStore.update(connection) { mStore =>
      val previous = Option(mStore.identityParams)
      val ledgerId = params.ledgerId
      val participantId = params.participantId
      previous match {
        case None =>
          logger.info(
            s"Initializing new database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
          )
          mStore.copy(identityParams = params)

        case Some(ParameterStorageBackend.IdentityParams(`ledgerId`, `participantId`)) =>
          logger.info(
            s"Found existing database for ledgerId '${params.ledgerId}' and participantId '${params.participantId}'"
          )
          mStore

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
    Option(MStore(connection)(_.identityParams))
}
