// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com
package daml.platform.store.backend.common

import daml.ledger.offset.Offset
import daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.Conversions.{offset, timestampFromMicros}
import daml.platform.store.backend.MeteringParameterStorageBackend
import daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import daml.scalautil.Statement.discard
import anorm.{RowParser, ~}
import com.daml.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd

import java.sql.Connection

private[backend] object MeteringParameterStorageBackendImpl
    extends MeteringParameterStorageBackend {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  def initializeLedgerMeteringEnd(
      init: LedgerMeteringEnd
  )(connection: Connection)(implicit loggingContext: LoggingContext): Unit = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    import com.daml.platform.store.backend.Conversions.TimestampToStatement
    ledgerMeteringEnd(connection) match {
      case None =>
        logger.info(s"Initializing ledger metering end to $init")
        discard(
          SQL"""insert into metering_parameters(
              ledger_metering_end,
              ledger_metering_timestamp
            ) values (
              ${init.offset},
              ${init.timestamp}
            )"""
            .execute()(connection)
        )
      case Some(existing) =>
        logger.info(s"Found existing ledger metering end $existing")
    }
  }

  def ledgerMeteringEnd(connection: Connection): Option[LedgerMeteringEnd] = {

    val LedgerMeteringEndParser: RowParser[LedgerMeteringEnd] = (
      offset("ledger_metering_end").?.map(_.getOrElse(Offset.beforeBegin)) ~
        timestampFromMicros("ledger_metering_timestamp")
    ) map { case ledgerMeteringEnd ~ ledgerMeteringTimestamp =>
      LedgerMeteringEnd(ledgerMeteringEnd, ledgerMeteringTimestamp)
    }

    SQL"""SELECT ledger_metering_end, ledger_metering_timestamp FROM metering_parameters"""
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
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    import com.daml.platform.store.backend.Conversions.TimestampToStatement
    SQL"""
        UPDATE
          metering_parameters
        SET
          ledger_metering_end = ${ledgerMeteringEnd.offset},
          ledger_metering_timestamp = ${ledgerMeteringEnd.timestamp}
        """
      .execute()(connection)
    ()
  }

}
