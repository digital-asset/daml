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
      val aSnapshotLastTs =
        CantonTimestamp.assertFromInstant(Instant.parse("2024-11-19T12:00:00.000Z"))

      forAll(
        Table(
          ("max topology store effective time", "expected effective time"),
          (None, aSnapshotLastTs),
          (Some(aSnapshotLastTs.minusMillis(1)), aSnapshotLastTs),
          (Some(aSnapshotLastTs.plusMillis(1)), aSnapshotLastTs.plusMillis(1)),
        )
      ) { case (maxTopologyStoreEffectiveTimeO, expectedHeadStateEffectiveTime) =>
        val topologyStoreMock = mock[TopologyStore[TopologyStoreId.DomainStore]]
        val topologyClientMock = mock[DomainTopologyClientWithInit]
        val initializer = new SequencerSnapshotBasedTopologyHeadInitializer(
          SequencerSnapshot(
            aSnapshotLastTs,
            latestBlockHeight = 77L,
            Map.empty,
            SequencerPruningStatus.Unimplemented,
            Map.empty,
            None,
            protocolVersion = testedProtocolVersion,
            trafficPurchased = Seq.empty,
            trafficConsumed = Seq.empty,
          ),
          topologyStoreMock,
        )

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
          .initialize(topologyClientMock)
          .map { _ =>
            verify(topologyClientMock).updateHead(
              SequencedTime(aSnapshotLastTs),
              EffectiveTime(expectedHeadStateEffectiveTime),
              ApproximateTime(aSnapshotLastTs),
              potentialTopologyChange = true,
            )
            succeed
          }
          .failOnShutdown
      }
    }
  }
}
