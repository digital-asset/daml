// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.{RowParser, ~}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.platform.store.backend.Conversions.{offset, timestampFromMicros}
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.tracing.TraceContext

import java.sql.Connection

private[backend] object MeteringParameterStorageBackendImpl
    extends MeteringParameterStorageBackend {

  def initializeLedgerMeteringEnd(
      init: LedgerMeteringEnd,
      loggerFactory: NamedLoggerFactory,
  )(connection: Connection)(implicit traceContext: TraceContext): Unit = {
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.TimestampToStatement
    ledgerMeteringEnd(connection) match {
      case None =>
        logger.debug(s"Initializing ledger metering end to $init")
        discard(
          SQL"""insert into lapi_metering_parameters(
              ledger_metering_end,
              ledger_metering_timestamp
            ) values (
              ${init.offset},
              ${init.timestamp}
            )"""
            .execute()(connection)
        )
      case Some(existing) =>
        logger.debug(s"Found existing ledger metering end $existing")
    }
  }

  def ledgerMeteringEnd(connection: Connection): Option[LedgerMeteringEnd] = {

    val LedgerMeteringEndParser: RowParser[LedgerMeteringEnd] = (
      offset("ledger_metering_end").?.map(_.getOrElse(Offset.beforeBegin)) ~
        timestampFromMicros("ledger_metering_timestamp")
    ) map { case ledgerMeteringEnd ~ ledgerMeteringTimestamp =>
      LedgerMeteringEnd(ledgerMeteringEnd, ledgerMeteringTimestamp)
    }

    SQL"""SELECT ledger_metering_end, ledger_metering_timestamp FROM lapi_metering_parameters"""
      .as(LedgerMeteringEndParser.singleOpt)(connection)

  }

  def assertLedgerMeteringEnd(connection: Connection): LedgerMeteringEnd = {
    ledgerMeteringEnd(connection).getOrElse(
      throw new IllegalStateException("Ledger metering is not initialized")
    )
  }

  def updateLedgerMeteringEnd(
      ledgerMeteringEnd: LedgerMeteringEnd
  )(connection: Connection): Unit = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.TimestampToStatement
    discard(
      SQL"""
        UPDATE
          lapi_metering_parameters
        SET
          ledger_metering_end = ${ledgerMeteringEnd.offset},
          ledger_metering_timestamp = ${ledgerMeteringEnd.timestamp}
        """
        .execute()(connection)
    )
  }

}
