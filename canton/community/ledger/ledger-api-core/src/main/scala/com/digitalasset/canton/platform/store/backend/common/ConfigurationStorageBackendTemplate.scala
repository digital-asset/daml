// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.RowParser
import anorm.SqlParser.{byteArray, flatten, str}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.store.backend.ConfigurationStorageBackend
import com.digitalasset.canton.platform.store.backend.Conversions.offset
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao.{acceptType, rejectType}
import com.digitalasset.canton.platform.store.entries.ConfigurationEntry

import java.sql.Connection

private[backend] class ConfigurationStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
) extends ConfigurationStorageBackend {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
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

  def ledgerConfiguration(connection: Connection): Option[(Offset, Configuration)] = {
    val ledgerEndOffset = ledgerEndCache()._1
    SQL"""
      select
        configuration_entries.ledger_offset,
        configuration_entries.recorded_at,
        configuration_entries.submission_id,
        configuration_entries.typ,
        configuration_entries.configuration,
        configuration_entries.rejection_reason
      from
        configuration_entries
      where
        configuration_entries.typ = '#$acceptType' and
        ${queryStrategy.offsetIsSmallerOrEqual(
        nonNullableColumn = "ledger_offset",
        endInclusive = ledgerEndOffset,
      )}
      order by ledger_offset desc
      fetch next 1 row only
  """
      .asVectorOf(configurationEntryParser)(connection)
      .collectFirst { case (offset, ConfigurationEntry.Accepted(_, configuration)) =>
        offset -> configuration
      }
  }

  def configurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, ConfigurationEntry)] = {
    SQL"""
      select
        configuration_entries.ledger_offset,
        configuration_entries.recorded_at,
        configuration_entries.submission_id,
        configuration_entries.typ,
        configuration_entries.configuration,
        configuration_entries.rejection_reason
      from
        configuration_entries
      where
        ${queryStrategy.offsetIsBetween(
        nonNullableColumn = "ledger_offset",
        startExclusive = startExclusive,
        endInclusive = endInclusive,
      )}
      order by ledger_offset asc
      offset $queryOffset rows
      fetch next $pageSize rows only
  """
      .asVectorOf(configurationEntryParser)(connection)
  }
}
