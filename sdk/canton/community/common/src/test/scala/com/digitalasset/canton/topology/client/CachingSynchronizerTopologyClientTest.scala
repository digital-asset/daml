// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveNumeric}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CacheConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  KeyCollection,
  SequencerGroup,
  TestingOwnerWithKeys,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, SequencerCounter, config}
import org.mockito.ArgumentMatcher
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.Future

object EffectiveTimeTestHelpers {

  import scala.language.implicitConversions

  implicit def toSequencedTime(ts: CantonTimestamp): SequencedTime = SequencedTime(ts)
  implicit def toEffectiveTime(ts: CantonTimestamp): EffectiveTime = EffectiveTime(ts)

}

class CachingSynchronizerTopologyClientTest
    extends AsyncWordSpecLike
    with BaseTest
    with FailOnShutdown {

  import EffectiveTimeTestHelpers.*

  private object Fixture {

    val owner = DefaultTestIdentities.sequencerId
    val crypto = new TestingOwnerWithKeys(owner, loggerFactory, directExecutionContext)
    val mockTransaction = mock[GenericSignedTopologyTransaction]

    val mockParent = mock[StoreBasedSynchronizerTopologyClient]
    val mockSnapshotEmpty = mock[StoreBasedTopologySnapshot]
    val mockGenesisSnapshot = mock[StoreBasedTopologySnapshot]
    val mockSnapshot0 = mock[StoreBasedTopologySnapshot]
    val mockSnapshot1 = mock[StoreBasedTopologySnapshot]
    val mockSnapshot2 = mock[StoreBasedTopologySnapshot]

    val key1 = crypto.SigningKeys.key1
    val key2 = crypto.SigningKeys.key2

    def seqGroup(threshold: Int) =
      FutureUnlessShutdown.pure(
        Some(
          SequencerGroup(
            active = Seq(),
            passive = Seq(),
            threshold = PositiveInt.tryCreate(threshold),
          )
        )
      )

    when(mockSnapshotEmpty.sequencerGroup()(any[TraceContext]))
      .thenReturn(seqGroup(1))
    when(mockGenesisSnapshot.sequencerGroup()(any[TraceContext]))
      .thenReturn(seqGroup(2))
    when(mockSnapshot0.sequencerGroup()(any[TraceContext]))
      .thenReturn(seqGroup(3))
    when(mockSnapshot1.sequencerGroup()(any[TraceContext]))
      .thenReturn(seqGroup(4))
    when(mockSnapshot2.sequencerGroup()(any[TraceContext]))
      .thenReturn(seqGroup(5))
    when(mockSnapshot0.allKeys(owner))
      .thenReturn(FutureUnlessShutdown.pure(KeyCollection(signingKeys = Seq(key1), Seq())))
    when(mockSnapshot1.allKeys(owner))
      .thenReturn(FutureUnlessShutdown.pure(KeyCollection(signingKeys = Seq(key1, key2), Seq())))
    when(mockSnapshot2.allKeys(owner))
      .thenReturn(FutureUnlessShutdown.pure(KeyCollection(signingKeys = Seq(key2), Seq())))

    def freshCachingClient(): CachingSynchronizerTopologyClient =
      new CachingSynchronizerTopologyClient(
        mockParent,
        CachingConfigs(
          topologySnapshot = CacheConfig(
            maximumSize = PositiveNumeric.tryCreate(100L),
            expireAfterAccess = config.NonNegativeFiniteDuration.ofMinutes(5),
          )
        ),
        BatchingConfig(),
        DefaultProcessingTimeouts.testing,
        FutureSupervisor.Noop,
        loggerFactory,
      )

    val ts0 = CantonTimestamp.Epoch
    val ts1 = ts0.plusSeconds(60)
    val ts2 = ts1.plusSeconds(60)
    val ts3 = ts2.plusSeconds(60)

    when(mockParent.topologyKnownUntilTimestamp).thenReturn(ts3.plusSeconds(3))
    when(mockParent.approximateTimestamp).thenReturn(ts3)
    when(mockParent.awaitTimestamp(any[CantonTimestamp])(any[TraceContext]))
      .thenReturn(None)
    when(
      mockParent.trySnapshot(eqTo(CantonTimestamp.MinValue.immediateSuccessor))(any[TraceContext])
    ).thenReturn(mockSnapshotEmpty)
    when(
      mockParent.trySnapshot(
        eqTo(SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor)
      )(any[TraceContext])
    ).thenReturn(mockGenesisSnapshot)
    when(mockParent.trySnapshot(eqTo(ts0))(any[TraceContext])).thenReturn(mockSnapshot0)
    when(mockParent.trySnapshot(eqTo(ts1))(any[TraceContext])).thenReturn(mockSnapshot0)
    when(mockParent.trySnapshot(eqTo(ts1.immediateSuccessor))(any[TraceContext]))
      .thenReturn(mockSnapshot1)
    when(mockParent.trySnapshot(eqTo(ts2.immediateSuccessor))(any[TraceContext]))
      .thenReturn(mockSnapshot2)
    when(
      mockParent.observed(
        any[CantonTimestamp],
        any[CantonTimestamp],
        any[SequencerCounter],
        anySeq[GenericSignedTopologyTransaction],
      )(any[TraceContext])
    ).thenReturn(FutureUnlessShutdown.unit)

  }

  class UpperInclusiveInterval(lowerInclusive: CantonTimestamp, upperExclusive: CantonTimestamp)
      extends ArgumentMatcher[CantonTimestamp] {
    override def matches(argument: CantonTimestamp): Boolean =
      argument > lowerInclusive && argument <= upperExclusive
  }

  "caching client" should {
    import Fixture.*

    "return correct snapshot" in {

      val cc = freshCachingClient()

      // 1. Empty store / setup
      when(mockParent.topologyKnownUntilTimestamp).thenReturn(
        CantonTimestamp.MinValue.immediateSuccessor
      )
      when(mockParent.latestTopologyChangeTimestamp).thenReturn(
        CantonTimestamp.MinValue.immediateSuccessor
      )
      for {
        // 1. Empty store / test
        emptyStoreSnapshot <- cc.snapshot(CantonTimestamp.MinValue.immediateSuccessor)
        // 1. Empty store / asserts
        res1 <- emptyStoreSnapshot.sequencerGroup()
        _ = res1.value.threshold.value shouldBe 1

        // 2. Genesis topology / setup
        _ = when(mockParent.topologyKnownUntilTimestamp).thenReturn(
          SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor
        )
        _ = when(mockParent.latestTopologyChangeTimestamp).thenReturn(
          SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor
        )
        // 2. Genesis topology / test
        genesisStoreSnapshot <- cc.snapshot(
          SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor
        )

        // 2. Genesis topology / asserts
        res2 <- genesisStoreSnapshot.sequencerGroup()
        _ = res2.value.threshold.value shouldBe 2

        // 3. Topology changes / setup
        _ <- cc
          .observed(ts1, ts1, SequencerCounter(1), Seq(mockTransaction))
        _ = when(mockParent.topologyKnownUntilTimestamp).thenReturn(ts1.immediateSuccessor)
        _ = when(mockParent.latestTopologyChangeTimestamp).thenReturn(ts1.immediateSuccessor)
        // ts0 and ts1 are not covered by the open interval [ts1.successor, None),
        // therefore will trigger a lookup of the topology interval for ts0 and ts1
        _ = when(
          mockParent.findTopologyIntervalForTimestamp(
            argThat(
              // Note: before exclusive, after inclusive!
              new UpperInclusiveInterval(CantonTimestamp.MinValue, ts1)
            )
          )(anyTraceContext)
        )
          .thenReturn(FutureUnlessShutdown.pure(None))
        _ = when(
          mockParent.findTopologyIntervalForTimestamp(
            argThat(
              new UpperInclusiveInterval(ts1, CantonTimestamp.MaxValue)
            )
          )(anyTraceContext)
        )
          .thenReturn(FutureUnlessShutdown.pure(Some((EffectiveTime(ts1), None))))

        // 3. Topology changes / test
        sp0a <- cc.snapshot(ts0)
        sp0b <- cc.snapshot(ts1)
        keys0a <- sp0a.allKeys(owner)
        keys0b <- sp0b.allKeys(owner)

        // 3. Topology changes / asserts
        _ = keys0a.signingKeys shouldBe Seq(key1)
        _ = keys0b.signingKeys shouldBe Seq(key1)

        // 4. Non-topology events / setup
        _ = cc.observed(ts1.plusSeconds(10), ts1.plusSeconds(10), SequencerCounter(1), Seq())
        _ = when(mockParent.topologyKnownUntilTimestamp).thenReturn(
          ts1.plusSeconds(10).immediateSuccessor
        )

        // 4. Non-topology events / test
        keys1a <- cc.snapshot(ts1.plusSeconds(5)).flatMap(_.allKeys(owner))

        // 4. Non-topology events / asserts
        _ = keys1a.signingKeys shouldBe Seq(key1, key2)

        // 5. More topology changes / setup
        _ = cc.observed(ts2, ts2, SequencerCounter(1), Seq(mockTransaction))
        _ = when(mockParent.topologyKnownUntilTimestamp).thenReturn(ts2.immediateSuccessor)
        _ = when(mockParent.latestTopologyChangeTimestamp).thenReturn(ts2.immediateSuccessor)
        // [ts1.successor, ts2.successor)
        _ = when(
          mockParent.findTopologyIntervalForTimestamp(
            argThat(
              new UpperInclusiveInterval(ts1, ts2)
            )
          )(anyTraceContext)
        )
          .thenReturn(
            FutureUnlessShutdown.pure(Some((EffectiveTime(ts1), Some(EffectiveTime(ts2)))))
          )
        // [ts2.successor, None)
        _ = when(
          mockParent.findTopologyIntervalForTimestamp(
            argThat(
              new UpperInclusiveInterval(ts2, CantonTimestamp.MaxValue)
            )
          )(anyTraceContext)
        )
          .thenReturn(FutureUnlessShutdown.pure(Some((EffectiveTime(ts2), None))))

        // 5. More topology changes / test
        keys1b <- cc.snapshot(ts2).flatMap(_.allKeys(owner))

        // 5. More topology changes / asserts
        _ = keys1b.signingKeys shouldBe Seq(key1, key2)

        // 6. More non-topology events / setup
        _ = cc.observed(ts3, ts3, SequencerCounter(1), Seq())
        _ = when(mockParent.topologyKnownUntilTimestamp).thenReturn(ts3.immediateSuccessor)

        // 6. More non-topology events / test
        keys2a <- cc.snapshot(ts2.plusSeconds(5)).flatMap(_.allKeys(owner))
        keys2b <- cc.snapshot(ts3).flatMap(_.allKeys(owner))

        // 6. More non-topology events / asserts
        _ = keys2a.signingKeys shouldBe Seq(key2)
        _ = keys2b.signingKeys shouldBe Seq(key2)
      } yield succeed

    }

    "verify we have properly cached our values" in {
      Future {
        verify(mockSnapshotEmpty, times(1)).sequencerGroup()
        verify(mockGenesisSnapshot, times(1)).sequencerGroup()
        verify(mockSnapshot0, times(2)).allKeys(owner)
        verify(mockSnapshot1, times(1)).allKeys(owner)
        verify(mockSnapshot2, times(1)).allKeys(owner)
        succeed
      }
    }
  }
}
