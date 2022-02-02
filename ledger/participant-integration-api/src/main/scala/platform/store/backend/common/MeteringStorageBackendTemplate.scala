// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.SqlParser.int
import anorm.{RowParser, ~}
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time
import com.daml.platform.store.Conversions.{applicationId, offset, timestampFromMicros}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.backend.MeteringStorageBackend
import com.daml.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
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

  override def transactionMetering(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      applicationId: Option[ApplicationId],
  )(connection: Connection): Vector[TransactionMetering] = {
    SQL"""
      select
        application_id,
        action_count,
        metering_timestamp,
        ledger_offset
      from transaction_metering
      where ledger_offset <= ${ledgerEndCache()._1.toHexString.toString}
      and   metering_timestamp >= ${from.micros}
      and   (${isSet(to)} = 0 or metering_timestamp < ${to.map(_.micros)})
      and   (${isSet(applicationId)} = 0 or application_id = ${applicationId.map(_.toString)})
    """
      .asVectorOf(transactionMeteringParser)(connection)
  }

  /** Oracle does not understand true/false so compare against number
    * @return 0 if the option is unset and non-zero otherwise
    */
  private def isSet(o: Option[_]): Int = o.fold(0)(_ => 1)

}
