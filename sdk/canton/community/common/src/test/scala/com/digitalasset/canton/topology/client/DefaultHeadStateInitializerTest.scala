// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class DefaultHeadStateInitializerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  "DefaultHeadStateInitializer" should {
    "initialize when topology store is non-empty" in {
      val topologyClientMock = mock[DomainTopologyClientWithInit]
      val topologyStoreMock = mock[TopologyStore[TopologyStoreId.DomainStore]]
      val initializer = new DefaultHeadStateInitializer(topologyStoreMock)

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
        .initialize(topologyClientMock)
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

    "not initialize when the topology store is empty" in {
      val topologyClientMock = mock[DomainTopologyClientWithInit]
      val topologyStoreMock = mock[TopologyStore[TopologyStoreId.DomainStore]]
      when(topologyStoreMock.maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true))
        .thenReturn(FutureUnlessShutdown.pure(None))
      val initializer = new DefaultHeadStateInitializer(topologyStoreMock)

      initializer
        .initialize(topologyClientMock)
        .map { _ =>
          verify(topologyClientMock, never).updateHead(
            any[SequencedTime],
            any[EffectiveTime],
            any[ApproximateTime],
            any[Boolean],
          )(any[TraceContext])
          succeed
        }
    }
  }
}
