// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex, SequencerIndex}
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

  it should "store and retrieve ledger end and domain indexes correctly" in {
    val someOffset = offset(1)
    val someSequencerTime = CantonTimestamp.now().plusSeconds(10)
    val someDomainIndex = DomainIndex.of(
      RequestIndex(
        counter = RequestCounter(10),
        sequencerCounter = Some(SequencerCounter(20)),
        timestamp = someSequencerTime,
      )
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.ledgerEnd) shouldBe LedgerEnd.beforeBegin
    executeSql(
      backend.parameter.cleanDomainIndex(StorageBackendTestValues.someSynchronizerId)
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
      backend.parameter.cleanDomainIndex(StorageBackendTestValues.someSynchronizerId2)
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

    // updateing ledger end and inserting one domain index
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = someOffset,
          lastEventSeqId = 1,
          lastStringInterningId = 1,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
        ),
        lastDomainIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someDomainIndex
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
    val resultDomainIndex = executeSql(
      backend.parameter.cleanDomainIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultDomainIndex.value.requestIndex shouldBe someDomainIndex.requestIndex
    resultDomainIndex.value.sequencerIndex shouldBe someDomainIndex.sequencerIndex
    resultDomainIndex.value.recordTime shouldBe someDomainIndex.recordTime

    // updating ledger end and inserting two domain index (one is updating just the request index part, the other is inserting just a sequencer index)
    val someDomainIndexSecond = DomainIndex.of(
      RequestIndex(
        counter = RequestCounter(11),
        sequencerCounter = None,
        timestamp = someSequencerTime.plusSeconds(10),
      )
    )
    val someDomainIndex2 = DomainIndex.of(
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
        lastDomainIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someDomainIndexSecond,
          StorageBackendTestValues.someSynchronizerId2 -> someDomainIndex2,
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
    val resultDomainIndexSecond = executeSql(
      backend.parameter.cleanDomainIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultDomainIndexSecond.value.requestIndex shouldBe someDomainIndexSecond.requestIndex
    resultDomainIndexSecond.value.sequencerIndex shouldBe someDomainIndex.sequencerIndex
    val resultDomainIndexSecond2 = executeSql(
      backend.parameter.cleanDomainIndex(StorageBackendTestValues.someSynchronizerId2)
    )
    resultDomainIndexSecond.value.recordTime shouldBe someDomainIndexSecond.recordTime
    resultDomainIndexSecond2.value.requestIndex shouldBe None
    resultDomainIndexSecond2.value.sequencerIndex shouldBe someDomainIndex2.sequencerIndex
    resultDomainIndexSecond2.value.recordTime shouldBe someDomainIndex2.recordTime

    // updating ledger end and inserting one domain index only overriding the record time
    val someDomainIndexThird = DomainIndex.of(
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
        lastDomainIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someDomainIndexThird
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
    val resultDomainIndexThird = executeSql(
      backend.parameter.cleanDomainIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultDomainIndexThird.value.requestIndex shouldBe someDomainIndexSecond.requestIndex
    resultDomainIndexThird.value.sequencerIndex shouldBe someDomainIndex.sequencerIndex
    resultDomainIndexThird.value.recordTime shouldBe someSequencerTime.plusSeconds(20)
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
