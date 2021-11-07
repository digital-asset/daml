// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.backend.ConfigurationStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.ConfigurationEntry

class MConfigurationStorageBackend(ledgerEndCache: LedgerEndCache)
    extends ConfigurationStorageBackend {
  override def ledgerConfiguration(connection: Connection): Option[(Offset, Configuration)] = {
    val ledgerEnd = ledgerEndCache()._1.toHexString
    MStore(connection).mData.configurations.reverseIterator
      .filter(_.ledger_offset <= ledgerEnd)
      .filter(_.typ == "accept")
      .take(1)
      .toList
      .headOption
      .map(dto =>
        Offset.fromHexString(Ref.HexString.assertFromString(dto.ledger_offset)) -> configuration(
          dto.configuration
        )
      )
  }

  override def configurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, ConfigurationEntry)] = {
    val startOffset = startExclusive.toHexString
    val endOffset = endInclusive.toHexString
    MStore(connection).mData.configurations.iterator
      .dropWhile(dto => startOffset.nonEmpty && dto.ledger_offset <= startOffset)
      .takeWhile(_.ledger_offset <= endOffset)
      .slice(queryOffset.toInt, queryOffset.toInt + pageSize)
      .map(dto =>
        (
          Offset.fromHexString(Ref.HexString.assertFromString(dto.ledger_offset)),
          if (dto.typ == "accept")
            ConfigurationEntry.Accepted(
              submissionId = dto.submission_id,
              configuration = configuration(dto.configuration),
            )
          else
            ConfigurationEntry.Rejected(
              submissionId = dto.submission_id,
              rejectionReason = dto.rejection_reason.getOrElse(""),
              proposedConfiguration = configuration(dto.configuration),
            ),
        )
      )
      .toVector
  }

  private def configuration(bytes: Array[Byte]): Configuration =
    Configuration
      .decode(bytes)
      .fold(err => sys.error(s"Failed to decode configuration: $err"), identity)
}
