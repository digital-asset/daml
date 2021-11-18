// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{flatten, str}
import anorm.{Macro, RowParser, SQL, SqlParser}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.platform.store.Conversions.{ledgerString, offset, timestampFromMicros}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.JdbcLedgerDao.{acceptType, rejectType}
import com.daml.platform.store.backend.PackageStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.PackageLedgerEntry

private[backend] class PackageStorageBackendTemplate(ledgerEndCache: LedgerEndCache)
    extends PackageStorageBackend {

  private val SQL_SELECT_PACKAGES =
    SQL(
      """select packages.package_id, packages.source_description, packages.known_since, packages.package_size
        |from packages
        |where packages.ledger_offset <= {ledgerEndOffset}
        |""".stripMargin
    )

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

  def lfPackages(connection: Connection): Map[PackageId, PackageDetails] =
    SQL_SELECT_PACKAGES
      .on("ledgerEndOffset" -> ledgerEndCache()._1.toHexString.toString)
      .as(PackageDataParser.*)(connection)
      .map(d =>
        PackageId.assertFromString(d.packageId) -> PackageDetails(
          d.size,
          Timestamp.assertFromLong(d.knownSince),
          d.sourceDescription,
        )
      )
      .toMap

  private val SQL_SELECT_PACKAGE =
    SQL("""select packages.package
          |from packages
          |where package_id = {package_id}
          |and packages.ledger_offset <= {ledgerEndOffset}
          |""".stripMargin)

  def lfArchive(packageId: PackageId)(connection: Connection): Option[Array[Byte]] = {
    import com.daml.platform.store.Conversions.packageIdToStatement
    SQL_SELECT_PACKAGE
      .on(
        "package_id" -> packageId,
        "ledgerEndOffset" -> ledgerEndCache()._1.toHexString.toString,
      )
      .as[Option[Array[Byte]]](SqlParser.byteArray("package").singleOpt)(connection)
  }

  private val SQL_GET_PACKAGE_ENTRIES = SQL(
    """select * from package_entries
      |where ({startExclusive} is null or ledger_offset>{startExclusive})
      |and ledger_offset<={endInclusive}
      |order by ledger_offset asc
      |offset {queryOffset} rows
      |fetch next {pageSize} rows only""".stripMargin
  )

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
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL_GET_PACKAGE_ENTRIES
      .on(
        "startExclusive" -> startExclusive,
        "endInclusive" -> endInclusive,
        "pageSize" -> pageSize,
        "queryOffset" -> queryOffset,
      )
      .asVectorOf(packageEntryParser)(connection)
  }

}
