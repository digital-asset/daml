// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.sequencing.protocol.{
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingOwnerWithKeys}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.*

class BftTopologyForInitDownloaderTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasCloseContext
    with FlagCloseable {

  "downloadAndVerifyTopologyTxs" should {

    val testMember = DefaultTestIdentities.mediatorId
    val testRequest = TopologyStateForInitRequest(testMember)(
      TopologyStateForInitRequest.protocolVersionRepresentativeFor(testedProtocolVersion)
    )
    val testingTransactions =
      new TestingOwnerWithKeys(testMember, loggerFactory, executorService).TestingTransactions
    val tx1 = StoredTopologyTransaction(
      sequenced = SequencedTime(CantonTimestamp.Epoch),
      validFrom = EffectiveTime(CantonTimestamp.Epoch),
      validUntil = None,
      transaction = testingTransactions.dpc1,
      rejectionReason = None,
    )
    val tx2 = StoredTopologyTransaction(
      sequenced = SequencedTime(CantonTimestamp.Epoch),
      validFrom = EffectiveTime(CantonTimestamp.Epoch),
      validUntil = None,
      transaction = testingTransactions.ns1k2,
      rejectionReason = None,
    )
    val validHash = {
      val hashBuilder = TopologyStateHash.build()
      hashBuilder.add(tx1)
      hashBuilder.add(tx2)

      TopologyStateForInitHashResponse(
        hashBuilder.finish().hash
      )
    }
    validHash: Unit
    val invalidHash = {
      val hashBuilder = TopologyStateHash.build()
      hashBuilder.add(tx1)
      TopologyStateForInitHashResponse(
        hashBuilder.finish().hash
      )
    }

    val testDownloadResponse: GenericStoredTopologyTransactions =
      StoredTopologyTransactions(
        Seq(
          tx1,
          tx2,
        )
      )

    implicit val closeContext: CloseContext = CloseContext(this)

    "fail on download mismatch with expected bft hash" in {

      val errorMessage = BftTopologyForInitDownloader
        .downloadAndVerifyTopologyTxs(
          maxRetries = 0,
          retryLogLevel = None,
          retryDelay = 1.second,
          request = testRequest,
          loggerFactory = loggerFactory,
          getBftInitTopologyStateHash =
            _ => EitherT.pure[FutureUnlessShutdown, String](invalidHash),
          downloadSnapshot = _ => EitherT.pure[FutureUnlessShutdown, String](testDownloadResponse),
        )
        .futureValueUS
        .leftOrFail("Expected a hash disagreement")

      errorMessage should include("Bft hash mismatch for downloaded topology state for init")
    }

    "succeed on download matching expected bft hash" in {

      val response = BftTopologyForInitDownloader
        .downloadAndVerifyTopologyTxs(
          maxRetries = 0,
          retryLogLevel = None,
          retryDelay = 1.second,
          request = testRequest,
          loggerFactory = loggerFactory,
          getBftInitTopologyStateHash = _ => EitherT.pure[FutureUnlessShutdown, String](validHash),
          downloadSnapshot = _ => EitherT.pure[FutureUnlessShutdown, String](testDownloadResponse),
        )
        .futureValueUS
        .valueOrFail("Expected a successful download")

      response shouldEqual testDownloadResponse
    }
  }
}
