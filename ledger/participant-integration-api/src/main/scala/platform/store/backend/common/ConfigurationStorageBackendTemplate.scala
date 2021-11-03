// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{byteArray, flatten, str}
import anorm.{RowParser, SQL}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.platform.store.Conversions.offset
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.JdbcLedgerDao.{acceptType, rejectType}
import com.daml.platform.store.backend.ConfigurationStorageBackend
import com.daml.platform.store.entries.ConfigurationEntry

private[backend] object ConfigurationStorageBackendTemplate extends ConfigurationStorageBackend {

  private val SQL_GET_CONFIGURATION_ENTRIES = SQL(
    """select
      |    configuration_entries.ledger_offset,
      |    configuration_entries.recorded_at,
      |    configuration_entries.submission_id,
      |    configuration_entries.typ,
      |    configuration_entries.configuration,
      |    configuration_entries.rejection_reason
      |  from
      |    configuration_entries,
      |    parameters
      |  where
      |    ({startExclusive} is null or ledger_offset>{startExclusive}) and
      |    ledger_offset <= {endInclusive} and
      |    parameters.ledger_end >= ledger_offset
      |  order by ledger_offset asc
      |  offset {queryOffset} rows
      |  fetch next {pageSize} rows only
      |  """.stripMargin
  )

  private val SQL_GET_LATEST_CONFIGURATION_ENTRY = SQL(
    s"""select
       |    configuration_entries.ledger_offset,
       |    configuration_entries.recorded_at,
       |    configuration_entries.submission_id,
       |    configuration_entries.typ,
       |    configuration_entries.configuration,
       |    configuration_entries.rejection_reason
       |  from
       |    configuration_entries,
       |    parameters
       |  where
       |    configuration_entries.typ = '$acceptType' and
       |    parameters.ledger_end >= ledger_offset
       |  order by ledger_offset desc
       |  fetch next 1 row only""".stripMargin
  )

  private val configurationEntryParser: RowParser[(Offset, ConfigurationEntry)] =
    (offset("ledger_offset") ~
      str("typ") ~
      str("submission_id") ~
      str("rejection_reason").map(s => if (s.isEmpty) null else s).? ~
      byteArray("configuration"))
      .map(flatten)
      .map { case (offset, typ, submissionId, rejectionReason, configBytes) =>
        val config = Configuration
          .decode(configBytes)
          .fold(err => sys.error(s"Failed to decode configuration: $err"), identity)
        offset ->
          (typ match {
            case `acceptType` =>
              ConfigurationEntry.Accepted(
                submissionId = submissionId,
                configuration = config,
              )
            case `rejectType` =>
              ConfigurationEntry.Rejected(
                submissionId = submissionId,
                rejectionReason = rejectionReason.getOrElse("<missing reason>"),
                proposedConfiguration = config,
              )

            case _ =>
              sys.error(s"getConfigurationEntries: Unknown configuration entry type: $typ")
          })
      }

  def ledgerConfiguration(connection: Connection): Option[(Offset, Configuration)] =
    SQL_GET_LATEST_CONFIGURATION_ENTRY
      .on()
      .asVectorOf(configurationEntryParser)(connection)
      .collectFirst { case (offset, ConfigurationEntry.Accepted(_, configuration)) =>
        offset -> configuration
      }

  def configurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, ConfigurationEntry)] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_GET_CONFIGURATION_ENTRIES
      .on(
        "startExclusive" -> startExclusive,
        "endInclusive" -> endInclusive,
        "pageSize" -> pageSize,
        "queryOffset" -> queryOffset,
      )
      .asVectorOf(configurationEntryParser)(connection)
  }
}
