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
        timestampFromMicros("metering_timestamp") ~
        offset("ledger_offset")
    ).map {
      case applicationId ~
          actionCount ~
          meteringTimestamp ~
          ledgerOffset =>
        TransactionMetering(
          applicationId,
          actionCount,
          meteringTimestamp,
          ledgerOffset,
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
        metering_timestamp,
        ledger_offset
      from
        transaction_metering
      where
        ledger_offset <= ${ledgerEndCache()._1.toHexString.toString}
      order by ledger_offset, application_id asc
    """
      .asVectorOf(transactionMeteringParser)(connection)
  }

}
