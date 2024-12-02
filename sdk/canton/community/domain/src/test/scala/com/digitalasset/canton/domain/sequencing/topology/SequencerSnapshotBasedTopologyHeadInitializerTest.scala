// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.topology

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.{
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class SequencerSnapshotBasedTopologyHeadInitializerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  "initialize" should {
    "update the head state using the sequencer snapshot" in {
      val aLastTs = CantonTimestamp.assertFromInstant(Instant.parse("2024-11-19T12:00:00.000Z"))

      forAll(
        Table(
          ("snapshot's lastTs", "max topology store effective time", "expected effective time"),
          (aLastTs, None, aLastTs),
          (aLastTs, Some(aLastTs.minusMillis(1)), aLastTs),
          (aLastTs, Some(aLastTs.plusMillis(1)), aLastTs.plusMillis(1)),
        )
      ) { case (lastTs, maxTopologyStoreEffectiveTimeO, expectedHeadStateEffectiveTime) =>
        val initializer = new SequencerSnapshotBasedTopologyHeadInitializer(
          Some(
            SequencerSnapshot(
              lastTs,
              latestBlockHeight = 77L,
              Map.empty,
              SequencerPruningStatus.Unimplemented,
              Map.empty,
              None,
              protocolVersion = testedProtocolVersion,
              trafficPurchased = Seq.empty,
              trafficConsumed = Seq.empty,
            )
          )
        )
        val topologyClientMock = mock[DomainTopologyClientWithInit]
        val topologyStoreMock = mock[TopologyStore[TopologyStoreId.DomainStore]]
        when(topologyStoreMock.maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true))
          .thenReturn(
            FutureUnlessShutdown.pure(
              maxTopologyStoreEffectiveTimeO.map(maxTopologyStoreEffectiveTime =>
                SequencedTime(CantonTimestamp.MinValue /* not used */ ) -> EffectiveTime(
                  maxTopologyStoreEffectiveTime
                )
              )
            )
          )

        initializer
          .initialize(topologyClientMock, topologyStoreMock)
          .map { _ =>
            verify(topologyClientMock).updateHead(
              SequencedTime(lastTs),
              EffectiveTime(expectedHeadStateEffectiveTime),
              ApproximateTime(lastTs),
              potentialTopologyChange = true,
            )
            succeed
          }
          .failOnShutdown
      }
    }

    "use the default initializer if a snapshot is not provided" in {
      val initializer = new SequencerSnapshotBasedTopologyHeadInitializer(None)
      val topologyClientMock = mock[DomainTopologyClientWithInit]
      val topologyStoreMock = mock[TopologyStore[TopologyStoreId.DomainStore]]
      val maxSequencedTimestamp =
        CantonTimestamp.assertFromInstant(Instant.parse("2024-11-19T12:00:00.000Z"))
      val maxEffectiveTimestamp = maxSequencedTimestamp.plusMillis(250)
      when(topologyStoreMock.maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true))
        .thenReturn(
          FutureUnlessShutdown.pure(
            Some(SequencedTime(maxSequencedTimestamp) -> EffectiveTime(maxEffectiveTimestamp))
          )
        )

      initializer
        .initialize(topologyClientMock, topologyStoreMock)
        .map { _ =>
          verify(topologyClientMock).updateHead(
            SequencedTime(maxSequencedTimestamp),
            EffectiveTime(maxEffectiveTimestamp),
            ApproximateTime(maxEffectiveTimestamp),
            potentialTopologyChange = true,
          )
          succeed
        }
    }
  }
}
