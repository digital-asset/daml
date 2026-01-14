// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.party

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.participant.admin.grpc.GrpcPartyManagementService
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.topology.SynchronizerId
import org.scalatest.wordspec.AnyWordSpec

/** Test the getHighestOffsetByTimestamp semantics of OffPR offset computation from timestamp.
  */
class OfflinePartyReplicationOffsetFromTimestampTest extends AnyWordSpec with BaseTest {

  private val cleanTimestampBefore = CantonTimestamp.ofEpochSecond(10L)
  private val requestSynchronizerOffsetBefore = CantonTimestamp.ofEpochSecond(20L)
  private val requestedTimestamp = CantonTimestamp.ofEpochSecond(20L)
  private val timestampAfter = CantonTimestamp.ofEpochSecond(30L)
  private val synchronizerId = SynchronizerId.tryFromString("synchronizer-id::ns")
  private val highestOffsetBeforeOrAtRequestedTimestamp =
    SynchronizerOffset(
      Offset.tryFromLong(100L),
      synchronizerId,
      recordTime = requestSynchronizerOffsetBefore.underlying,
      publicationTime = requestSynchronizerOffsetBefore.underlying,
    )
  private val ledgerEnd = LedgerEnd(
    lastOffset = Offset.tryFromLong(105L),
    lastEventSeqId = 105L, // dummy value
    lastStringInterningId = 1, // dummy value
    lastPublicationTime = CantonTimestamp.ofEpochSecond(
      40L
    ),
  )

  "offline party replication offset-from-timestamp computation" should {
    "return the retrieved offset if requested timestamp is clean" in {
      val offsetE = GrpcPartyManagementService.identifyHighestOffsetByTimestamp(
        requestedTimestamp = requestedTimestamp,
        synchronizerOffsetBeforeOrAtRequestedTimestamp = highestOffsetBeforeOrAtRequestedTimestamp,
        forceFlag = false,
        cleanSynchronizerTimestamp = timestampAfter,
        ledgerEnd = ledgerEnd,
        synchronizerId,
      )
      offsetE shouldBe Right(highestOffsetBeforeOrAtRequestedTimestamp.offset)
    }

    "error if the requested timestamp is not clean" in {
      val error = GrpcPartyManagementService
        .identifyHighestOffsetByTimestamp(
          requestedTimestamp = requestedTimestamp,
          synchronizerOffsetBeforeOrAtRequestedTimestamp =
            highestOffsetBeforeOrAtRequestedTimestamp,
          forceFlag = false,
          cleanSynchronizerTimestamp = cleanTimestampBefore,
          ledgerEnd = ledgerEnd,
          synchronizerId,
        )
        .left
        .value
      error shouldBe a[PartyManagementServiceError.InvalidTimestamp.Error]
      error.cause should include(
        "Not all events have been processed fully and/or published to the Ledger API DB until the requested timestamp:"
      )
    }

    "if forced with subsequent transactions, return the retrieved offset" in {
      val offsetE = GrpcPartyManagementService.identifyHighestOffsetByTimestamp(
        requestedTimestamp = requestedTimestamp,
        synchronizerOffsetBeforeOrAtRequestedTimestamp = highestOffsetBeforeOrAtRequestedTimestamp,
        forceFlag = true,
        cleanSynchronizerTimestamp = timestampAfter,
        ledgerEnd = ledgerEnd,
        synchronizerId,
      )
      offsetE shouldBe Right(highestOffsetBeforeOrAtRequestedTimestamp.offset)
    }

    "if forced, but no subsequent transactions, return the ledger end offset even if requested timestamp is not clean" in {
      val offsetE = GrpcPartyManagementService.identifyHighestOffsetByTimestamp(
        requestedTimestamp = requestedTimestamp,
        synchronizerOffsetBeforeOrAtRequestedTimestamp = highestOffsetBeforeOrAtRequestedTimestamp,
        forceFlag = true,
        cleanSynchronizerTimestamp = cleanTimestampBefore,
        ledgerEnd = ledgerEnd,
        synchronizerId,
      )
      offsetE shouldBe Right(ledgerEnd.lastOffset)
    }

    "refuse to emit a synchronizer offset with a record time after the requested timestamp" in {
      val synchronizerOffsetTooLarge = highestOffsetBeforeOrAtRequestedTimestamp
        .copy(recordTime = requestedTimestamp.immediateSuccessor.underlying)

      val error =
        GrpcPartyManagementService
          .identifyHighestOffsetByTimestamp(
            requestedTimestamp = requestedTimestamp,
            synchronizerOffsetBeforeOrAtRequestedTimestamp = synchronizerOffsetTooLarge,
            forceFlag = true,
            cleanSynchronizerTimestamp = timestampAfter,
            ledgerEnd = ledgerEnd,
            synchronizerId,
          )
          .left
          .value
      error shouldBe a[PartyManagementServiceError.InvalidTimestamp.Error]
      error.cause should include regex "Coding bug: Returned offset record time .* must be before or at the requested timestamp"
    }

    "error if the synchronizer offset if larger than the ledger end offset" in {
      val synchronizerOffsetLargerThanLedgerEnd = highestOffsetBeforeOrAtRequestedTimestamp
        .copy(offset = ledgerEnd.lastOffset.increment)

      val error =
        GrpcPartyManagementService
          .identifyHighestOffsetByTimestamp(
            requestedTimestamp = requestedTimestamp,
            synchronizerOffsetBeforeOrAtRequestedTimestamp = synchronizerOffsetLargerThanLedgerEnd,
            forceFlag = false,
            cleanSynchronizerTimestamp = timestampAfter,
            ledgerEnd = ledgerEnd,
            synchronizerId,
          )
          .left
          .value
      error shouldBe a[PartyManagementServiceError.InvalidTimestamp.Error]
      error.cause should include regex "The synchronizer offset .* is not less than or equal to the ledger end offset"
    }
  }
}
