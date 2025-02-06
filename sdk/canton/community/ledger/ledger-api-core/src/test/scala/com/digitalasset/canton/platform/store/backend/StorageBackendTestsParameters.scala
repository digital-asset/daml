// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{
  RequestIndex,
  SequencerIndex,
  SynchronizerIndex,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.{HasExecutionContext, RequestCounter, SequencerCounter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsParameters
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend Parameters"

  import StorageBackendTestValues.*

  it should "store and retrieve ledger end and synchronizer indexes correctly" in {
    val someOffset = offset(1)
    val someSequencerTime = CantonTimestamp.now().plusSeconds(10)
    val someSynchronizerIndex = SynchronizerIndex.of(
      RequestIndex(
        counter = RequestCounter(10),
        sequencerCounter = Some(SequencerCounter(20)),
        timestamp = someSequencerTime,
      )
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.ledgerEnd) shouldBe LedgerEnd.beforeBegin
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    ) shouldBe None
    val someSynchronizerIdInterned =
      backend.stringInterningSupport.synchronizerId.internalize(
        StorageBackendTestValues.someSynchronizerId
      )
    executeSql(connection =>
      ingest(
        Vector(
          DbDto.StringInterningDto(
            someSynchronizerIdInterned,
            "d|" + StorageBackendTestValues.someSynchronizerId.toProtoPrimitive,
          )
        ),
        connection,
      )
    )
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId2)
    ) shouldBe None
    val someSynchronizerIdInterned2 =
      backend.stringInterningSupport.synchronizerId.internalize(
        StorageBackendTestValues.someSynchronizerId2
      )
    executeSql(connection =>
      ingest(
        Vector(
          DbDto.StringInterningDto(
            someSynchronizerIdInterned2,
            "d|" + StorageBackendTestValues.someSynchronizerId2.toProtoPrimitive,
          )
        ),
        connection,
      )
    )

    // updateing ledger end and inserting one synchronizer index
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = someOffset,
          lastEventSeqId = 1,
          lastStringInterningId = 1,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
        ),
        lastSynchronizerIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndex
        ),
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = someOffset,
        lastEventSeqId = 1,
        lastStringInterningId = 1,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
      )
    )
    val resultSynchronizerIndex = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultSynchronizerIndex.value.requestIndex shouldBe someSynchronizerIndex.requestIndex
    resultSynchronizerIndex.value.sequencerIndex shouldBe someSynchronizerIndex.sequencerIndex
    resultSynchronizerIndex.value.recordTime shouldBe someSynchronizerIndex.recordTime

    // updating ledger end and inserting two synchronizer index (one is updating just the request index part, the other is inserting just a sequencer index)
    val someSynchronizerIndexSecond = SynchronizerIndex.of(
      RequestIndex(
        counter = RequestCounter(11),
        sequencerCounter = None,
        timestamp = someSequencerTime.plusSeconds(10),
      )
    )
    val someSynchronizerIndex2 = SynchronizerIndex.of(
      SequencerIndex(
        counter = SequencerCounter(3),
        timestamp = someSequencerTime.plusSeconds(5),
      )
    )
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = offset(100),
          lastEventSeqId = 100,
          lastStringInterningId = 100,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(100),
        ),
        lastSynchronizerIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexSecond,
          StorageBackendTestValues.someSynchronizerId2 -> someSynchronizerIndex2,
        ),
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = offset(100),
        lastEventSeqId = 100,
        lastStringInterningId = 100,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(100),
      )
    )
    val resultSynchronizerIndexSecond = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultSynchronizerIndexSecond.value.requestIndex shouldBe someSynchronizerIndexSecond.requestIndex
    resultSynchronizerIndexSecond.value.sequencerIndex shouldBe someSynchronizerIndex.sequencerIndex
    val resultSynchronizerIndexSecond2 = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId2)
    )
    resultSynchronizerIndexSecond.value.recordTime shouldBe someSynchronizerIndexSecond.recordTime
    resultSynchronizerIndexSecond2.value.requestIndex shouldBe None
    resultSynchronizerIndexSecond2.value.sequencerIndex shouldBe someSynchronizerIndex2.sequencerIndex
    resultSynchronizerIndexSecond2.value.recordTime shouldBe someSynchronizerIndex2.recordTime

    // updating ledger end and inserting one synchronizer index only overriding the record time
    val someSynchronizerIndexThird = SynchronizerIndex.of(
      someSequencerTime.plusSeconds(20)
    )
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = offset(200),
          lastEventSeqId = 200,
          lastStringInterningId = 200,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(200),
        ),
        lastSynchronizerIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexThird
        ),
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = offset(200),
        lastEventSeqId = 200,
        lastStringInterningId = 200,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(200),
      )
    )
    val resultSynchronizerIndexThird = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultSynchronizerIndexThird.value.requestIndex shouldBe someSynchronizerIndexSecond.requestIndex
    resultSynchronizerIndexThird.value.sequencerIndex shouldBe someSynchronizerIndex.sequencerIndex
    resultSynchronizerIndexThird.value.recordTime shouldBe someSequencerTime.plusSeconds(20)
  }

  it should "store and retrieve post processing end correctly" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.postProcessingEnd) shouldBe None
    executeSql(backend.parameter.updatePostProcessingEnd(Some(offset(10))))
    executeSql(backend.parameter.postProcessingEnd) shouldBe Some(offset(10))
    executeSql(backend.parameter.updatePostProcessingEnd(Some(offset(20))))
    executeSql(backend.parameter.postProcessingEnd) shouldBe Some(offset(20))
    executeSql(backend.parameter.updatePostProcessingEnd(None))
    executeSql(backend.parameter.postProcessingEnd) shouldBe None
  }
}
