// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.topology

import cats.data.EitherT
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.SequencedEventStore.{OrdinarySequencedEvent, SearchCriterion}
import com.digitalasset.canton.store.{SequencedEventNotFoundError, SequencedEventStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class SequencedEventStoreBasedTopologyHeadInitializerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  "initialize" should {
    "update the head state" in {
      val aTimestamp =
        CantonTimestamp.assertFromInstant(Instant.parse("2024-12-03T12:00:00.000Z"))

      val anEffectiveTime = aTimestamp.plusMillis(250)

      forAll(
        Table(
          (
            "latest sequenced event timestamp",
            "max topology store sequenced timestamps",
            "expected sequenced time",
            "expected effective time",
          ),
          (Some(aTimestamp), None, aTimestamp, aTimestamp),
          (None, Some(aTimestamp -> anEffectiveTime), aTimestamp, anEffectiveTime),
          (
            Some(aTimestamp),
            Some(aTimestamp -> anEffectiveTime),
            aTimestamp,
            anEffectiveTime,
          ),
          (
            Some(aTimestamp),
            Some(aTimestamp -> aTimestamp.minusSeconds(1)),
            aTimestamp,
            aTimestamp,
          ),
          (
            Some(aTimestamp),
            Some(aTimestamp.minusMillis(250) -> aTimestamp.plusSeconds(1)),
            aTimestamp.minusMillis(250),
            aTimestamp.plusSeconds(1),
          ),
        )
      ) {
        case (
              latestSequencedEventTimestampO,
              maxTopologyStoreTimestampsO,
              expectedHeadStateSequencedTime,
              expectedHeadStateEffectiveTime,
            ) =>
          val sequencedEventStoreMock = mock[SequencedEventStore]
          when(sequencedEventStoreMock.find(SearchCriterion.Latest))
            .thenReturn(latestSequencedEventTimestampO match {
              case Some(timestamp) =>
                EitherT.rightT(
                  OrdinarySequencedEvent(
                    SignedContent(
                      Deliver.create(
                        SequencerCounter(0),
                        timestamp,
                        DomainId.tryFromString("namespace::id"),
                        None,
                        Batch.empty(testedProtocolVersion),
                        None,
                        testedProtocolVersion,
                        Option.empty[TrafficReceipt],
                      ),
                      SymbolicCrypto.emptySignature,
                      None,
                      testedProtocolVersion,
                    )
                  )(TraceContext.empty)
                )
              case None =>
                EitherT.leftT(SequencedEventNotFoundError(SearchCriterion.Latest))
            })

          val topologyClientMock = mock[DomainTopologyClientWithInit]
          val topologyStoreMock = mock[TopologyStore[TopologyStoreId.DomainStore]]
          val initializer = new SequencedEventStoreBasedTopologyHeadInitializer(
            sequencedEventStoreMock,
            topologyStoreMock,
          )

          when(topologyStoreMock.maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true))
            .thenReturn(
              FutureUnlessShutdown.pure(
                maxTopologyStoreTimestampsO.map { case (sequencedTime, effectiveTime) =>
                  SequencedTime(sequencedTime) -> EffectiveTime(effectiveTime)
                }
              )
            )

          initializer
            .initialize(topologyClientMock)
            .map { _ =>
              verify(topologyClientMock).updateHead(
                SequencedTime(expectedHeadStateSequencedTime),
                EffectiveTime(expectedHeadStateEffectiveTime),
                ApproximateTime(expectedHeadStateEffectiveTime),
                potentialTopologyChange = true,
              )
              succeed
            }
            .failOnShutdown
      }
    }
  }
}
