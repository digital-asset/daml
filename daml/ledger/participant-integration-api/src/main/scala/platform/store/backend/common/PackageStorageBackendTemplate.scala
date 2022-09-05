// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{flatten, str}
import anorm.{Macro, RowParser, SqlParser}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.platform.store.backend.Conversions.{ledgerString, offset, timestampFromMicros}
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.PackageId
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.dao.JdbcLedgerDao.{acceptType, rejectType}
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.PackageStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.PackageLedgerEntry

private[backend] class PackageStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
) extends PackageStorageBackend {

  private case class ParsedPackageData(
      packageId: String,
      sourceDescription: Option[String],
      size: Long,
      knownSince: Long,
  )

  private val PackageDataParser: RowParser[ParsedPackageData] =
    Macro.parser[ParsedPackageData](
      "package_id",
      "source_description",
      "package_size",
      "known_since",
    )

  def lfPackages(connection: Connection): Map[PackageId, PackageDetails] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    val ledgerEndOffset = ledgerEndCache()._1
    SQL"""
      select packages.package_id, packages.source_description, packages.known_since, packages.package_size
      from packages
      where packages.ledger_offset <= $ledgerEndOffset
    """
      .as(PackageDataParser.*)(connection)
      .map(d =>
        PackageId.assertFromString(d.packageId) -> PackageDetails(
          d.size,
          Timestamp.assertFromLong(d.knownSince),
          d.sourceDescription,
        )
      )
      .toMap
  }

  def lfArchive(packageId: PackageId)(connection: Connection): Option[Array[Byte]] = {
    import com.daml.platform.store.backend.Conversions.packageIdToStatement
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    val ledgerEndOffset = ledgerEndCache()._1
    SQL"""
      select packages.package
      from packages
      where package_id = $packageId
      and packages.ledger_offset <= $ledgerEndOffset
    """
      .as[Option[Array[Byte]]](SqlParser.byteArray("package").singleOpt)(connection)
  }

  private val packageEntryParser: RowParser[(Offset, PackageLedgerEntry)] =
    (offset("ledger_offset") ~
      timestampFromMicros("recorded_at") ~
      ledgerString("submission_id").? ~
      str("typ") ~
      str("rejection_reason").?)
      .map(flatten)
      .map {
        case (offset, recordTime, Some(submissionId), `acceptType`, None) =>
          offset ->
            PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime)
        case (offset, recordTime, Some(submissionId), `rejectType`, Some(reason)) =>
          offset ->
            PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason)
        case invalidRow =>
          sys.error(s"packageEntryParser: invalid party entry row: $invalidRow")
      }

  def packageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PackageLedgerEntry)] = {
    SQL"""
      select * from package_entries
      where ${queryStrategy.offsetIsBetween(
        nonNullableColumn = "ledger_offset",
        startExclusive = startExclusive,
        endInclusive = endInclusive,
      )}
      order by ledger_offset asc
      offset $queryOffset rows
      fetch next $pageSize rows only
    """
      .asVectorOf(packageEntryParser)(connection)
  }

}
