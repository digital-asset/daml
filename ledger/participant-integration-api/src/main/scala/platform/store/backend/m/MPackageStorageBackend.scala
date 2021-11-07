// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Ref.PackageId
import com.daml.platform.store.backend.PackageStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.PackageLedgerEntry

class MPackageStorageBackend(ledgerEndCache: LedgerEndCache) extends PackageStorageBackend {
  override def lfPackages(connection: Connection): Map[PackageId, PackageDetails] = {
    val ledgerEndOffset = ledgerEndCache()._1.toHexString
    MStore(connection).mData.packages.iterator
      .filter(_._2.ledger_offset <= ledgerEndOffset)
      .map(dto =>
        (
          Ref.PackageId.assertFromString(dto._2.package_id),
          PackageDetails(
            size = dto._2.package_size,
            knownSince = Time.Timestamp(dto._2.known_since),
            sourceDescription = dto._2.source_description,
          ),
        )
      )
      .toMap
  }

  override def lfArchive(packageId: PackageId)(connection: Connection): Option[Array[Byte]] = {
    val ledgerEndOffset = ledgerEndCache()._1.toHexString
    MStore(connection).mData.packages
      .get(packageId.toString)
      .filter(_.ledger_offset <= ledgerEndOffset)
      .map(_._package)
  }

  override def packageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PackageLedgerEntry)] = {
    val startOffset = startExclusive.toHexString
    val endOffset = endInclusive.toHexString
    MStore(connection).mData.packageEntries.iterator
      .dropWhile(dto => startOffset.nonEmpty && dto.ledger_offset <= startOffset)
      .takeWhile(_.ledger_offset <= endOffset)
      .slice(queryOffset.toInt, queryOffset.toInt + pageSize)
      .map(dto =>
        (
          Offset.fromHexString(Ref.HexString.assertFromString(dto.ledger_offset)),
          if (dto.typ == "accept")
            PackageLedgerEntry.PackageUploadAccepted(
              submissionId = Ref.SubmissionId.assertFromString(dto.submission_id.get),
              recordTime = Time.Timestamp(dto.recorded_at),
            )
          else
            PackageLedgerEntry.PackageUploadRejected(
              submissionId = Ref.SubmissionId.assertFromString(dto.submission_id.get),
              recordTime = Time.Timestamp(dto.recorded_at),
              reason = dto.rejection_reason.get,
            ),
        )
      )
      .toVector
  }
}
