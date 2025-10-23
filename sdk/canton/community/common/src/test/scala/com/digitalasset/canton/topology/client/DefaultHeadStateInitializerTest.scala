// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      val topologyClientMock = mock[SynchronizerTopologyClientWithInit]
      when(topologyClientMock.initialize()(anyTraceContext))
        .thenReturn(FutureUnlessShutdown.unit)
      val topologyStoreMock = mock[TopologyStore[TopologyStoreId.SynchronizerStore]]
      val initializer = new DefaultHeadStateInitializer(topologyStoreMock)

      val maxSequencedTimestamp =
        CantonTimestamp.assertFromInstant(Instant.parse("2024-11-19T12:00:00.000Z"))
      val maxEffectiveTimestamp = maxSequencedTimestamp.plusMillis(250)
      when(
        topologyStoreMock.maxTimestamp(
          eqTo(SequencedTime.MaxValue),
          includeRejected = anyBoolean,
        )(anyTraceContext)
      )
        .thenReturn(
          FutureUnlessShutdown.pure(
            Some(SequencedTime(maxSequencedTimestamp) -> EffectiveTime(maxEffectiveTimestamp))
          )
        )

      initializer
        .initialize(
          topologyClientMock,
          synchronizerPredecessor = None,
          defaultStaticSynchronizerParameters,
        )
        .map { _ =>
          verify(topologyClientMock).updateHead(
            SequencedTime(maxSequencedTimestamp),
            EffectiveTime(maxEffectiveTimestamp),
            ApproximateTime(maxEffectiveTimestamp),
          )
          verify(topologyClientMock).initialize()(anyTraceContext)
          succeed
        }
    }

    "not initialize when the topology store is empty" in {
      val topologyClientMock = mock[SynchronizerTopologyClientWithInit]
      when(topologyClientMock.initialize()(anyTraceContext))
        .thenReturn(FutureUnlessShutdown.unit)
      val topologyStoreMock = mock[TopologyStore[TopologyStoreId.SynchronizerStore]]
      when(
        topologyStoreMock.maxTimestamp(
          eqTo(SequencedTime.MaxValue),
          includeRejected = anyBoolean,
        )(anyTraceContext)
      )
        .thenReturn(FutureUnlessShutdown.pure(None))
      val initializer = new DefaultHeadStateInitializer(topologyStoreMock)

      initializer
        .initialize(
          topologyClientMock,
          synchronizerPredecessor = None,
          defaultStaticSynchronizerParameters,
        )
        .map { _ =>
          verify(topologyClientMock, never).updateHead(
            any[SequencedTime],
            any[EffectiveTime],
            any[ApproximateTime],
          )(any[TraceContext])
          verify(topologyClientMock).initialize()(anyTraceContext)
          succeed
        }
    }
  }
}
