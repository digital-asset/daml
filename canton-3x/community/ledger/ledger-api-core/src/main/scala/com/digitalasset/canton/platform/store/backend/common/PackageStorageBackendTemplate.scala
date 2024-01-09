// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{flatten, str}
import anorm.{Macro, RowParser, SqlParser}
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.PackageDetails
import com.digitalasset.canton.platform.PackageId
import com.digitalasset.canton.platform.store.backend.Conversions.{
  ledgerString,
  offset,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.PackageStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao.{acceptType, rejectType}
import com.digitalasset.canton.platform.store.entries.PackageLedgerEntry

import java.sql.Connection

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
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
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
    import com.digitalasset.canton.platform.store.backend.Conversions.packageIdToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
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
