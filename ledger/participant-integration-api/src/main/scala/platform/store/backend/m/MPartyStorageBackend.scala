// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.api.domain
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.backend.{DbDto, PartyStorageBackend}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.PartyLedgerEntry

class MPartyStorageBackend(ledgerEndCache: LedgerEndCache) extends PartyStorageBackend {
  override def partyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PartyLedgerEntry)] = {
    val startOffset = startExclusive.toHexString
    val endOffset = endInclusive.toHexString
    MStore(connection).mData.partyEntries.iterator
      .dropWhile(dto => startOffset.nonEmpty && dto.ledger_offset <= startOffset)
      .takeWhile(_.ledger_offset <= endOffset)
      .slice(queryOffset.toInt, queryOffset.toInt + pageSize)
      .map(dto =>
        (
          Offset.fromHexString(Ref.HexString.assertFromString(dto.ledger_offset)),
          if (dto.typ == "accept")
            PartyLedgerEntry.AllocationAccepted(
              submissionIdOpt = dto.submission_id.map(Ref.SubmissionId.assertFromString),
              recordTime = Timestamp(dto.recorded_at),
              partyDetails = partyDetails(dto),
            )
          else
            PartyLedgerEntry.AllocationRejected(
              submissionId = Ref.SubmissionId.assertFromString(dto.submission_id.get),
              recordTime = Timestamp(dto.recorded_at),
              reason = dto.rejection_reason.get,
            ),
        )
      )
      .toVector
  }

  override def parties(parties: Seq[Ref.Party])(connection: Connection): List[domain.PartyDetails] =
    queryParties(Some(parties.view.map(_.toString).toSet), connection).toList

  override def knownParties(connection: Connection): List[domain.PartyDetails] =
    queryParties(None, connection).toList

  private def queryParties(
      parties: Option[Set[String]],
      connection: Connection,
  ): Vector[domain.PartyDetails] = {
    val ledgerEndOffset = ledgerEndCache()._1.toHexString
    val data = MStore(connection).mData
    (parties match {
      case Some(parties) =>
        parties.iterator
          .flatMap(p => data.partyIndex.get(p).iterator)
      case None =>
        data.partyIndex.iterator
          .map(_._2)
    })
      .flatMap { parties =>
        parties.reverseIterator
          .find(_.ledger_offset <= ledgerEndOffset)
          .iterator
          .map(party =>
            if (
              !party.is_local.get && parties.exists(p =>
                p.typ == "accept" &&
                  p.ledger_offset <= ledgerEndOffset &&
                  p.is_local.get
              )
            ) party.copy(is_local = Some(true))
            else party
          )
      }
      .map(partyDetails)
      .toVector
  }

  private def partyDetails(dto: DbDto.PartyEntry): domain.PartyDetails =
    domain.PartyDetails(
      party = Ref.Party.assertFromString(dto.party.get),
      displayName = dto.display_name,
      isLocal = dto.is_local.get,
    )
}
