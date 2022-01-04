// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.SqlParser.int
import anorm.{RowParser, ~}
import com.daml.platform.store.Conversions.{applicationId, offset, timestampFromMicros}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.backend.MeteringStorageBackend
import com.daml.platform.store.backend.MeteringStorageBackend.TransactionMetering
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.cache.LedgerEndCache

import java.sql.Connection

private[backend] class MeteringStorageBackendTemplate(ledgerEndCache: LedgerEndCache)
    extends MeteringStorageBackend {

  private val transactionMeteringParser: RowParser[TransactionMetering] = {
    (
        applicationId("application_id") ~
        int("action_count") ~
        timestampFromMicros("from_timestamp") ~
        timestampFromMicros("to_timestamp") ~
        offset("from_ledger_offset") ~
        offset("to_ledger_offset")
    ).map {
      case applicationId ~
          actionCount ~
          fromTimestamp ~
          toTimestamp ~
          fromLedgerOffset ~
          toLedgerOffset =>
        TransactionMetering(
          applicationId,
          actionCount,
          fromTimestamp,
          toTimestamp,
          fromLedgerOffset,
          toLedgerOffset,
        )
    }

  }

  override def entries(
      connection: Connection
  ): Vector[MeteringStorageBackend.TransactionMetering] = {
    SQL"""
      select
        application_id,
        action_count,
        from_timestamp,
        to_timestamp,
        from_ledger_offset,
        to_ledger_offset
      from
        transaction_metering
      where
        from_ledger_offset <= ${ledgerEndCache()._1.toHexString.toString}
      order by from_ledger_offset asc
    """
      .asVectorOf(transactionMeteringParser)(connection)
  }

}
