// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  BatchingConfig,
  CacheConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  KeyCollection,
  TestingOwnerWithKeysX,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SequencerCounter, config}
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.Future

object EffectiveTimeTestHelpers {

  import scala.language.implicitConversions

  implicit def toSequencedTime(ts: CantonTimestamp): SequencedTime = SequencedTime(ts)
  implicit def toEffectiveTime(ts: CantonTimestamp): EffectiveTime = EffectiveTime(ts)

}

class CachingDomainTopologyClientTest extends AsyncWordSpecLike with BaseTest {

  import EffectiveTimeTestHelpers.*

  private object Fixture {

    val owner = DefaultTestIdentities.sequencerIdX
    val crypto = new TestingOwnerWithKeysX(owner, loggerFactory, directExecutionContext)
    val mockTransaction = mock[GenericSignedTopologyTransactionX]

    val mockParent = mock[DomainTopologyClientWithInit]
    val mockSnapshot0 = mock[TopologySnapshotLoader]
    val mockSnapshot1 = mock[TopologySnapshotLoader]
    val mockSnapshot2 = mock[TopologySnapshotLoader]

    val key1 = crypto.SigningKeys.key1
    val key2 = crypto.SigningKeys.key2

    when(mockSnapshot0.allKeys(owner))
      .thenReturn(Future.successful(KeyCollection(signingKeys = Seq(key1), Seq())))
    when(mockSnapshot1.allKeys(owner))
      .thenReturn(Future.successful(KeyCollection(signingKeys = Seq(key1, key2), Seq())))
    when(mockSnapshot2.allKeys(owner))
      .thenReturn(Future.successful(KeyCollection(signingKeys = Seq(key2), Seq())))

    val cc =
      new CachingDomainTopologyClient(
        mock[Clock],
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

    val ts1 = CantonTimestamp.Epoch
    val ts0 = ts1.minusSeconds(60)
    val ts2 = ts1.plusSeconds(60)
    val ts3 = ts2.plusSeconds(60)

    when(mockParent.topologyKnownUntilTimestamp).thenReturn(ts3.plusSeconds(3))
    when(mockParent.approximateTimestamp).thenReturn(ts3)
    when(mockParent.awaitTimestamp(any[CantonTimestamp], any[Boolean])(any[TraceContext]))
      .thenReturn(None)
    when(mockParent.trySnapshot(ts0)).thenReturn(mockSnapshot0)
    when(mockParent.trySnapshot(ts1)).thenReturn(mockSnapshot0)
    when(mockParent.trySnapshot(ts1.immediateSuccessor)).thenReturn(mockSnapshot1)
    when(mockParent.trySnapshot(ts2.immediateSuccessor)).thenReturn(mockSnapshot2)
    when(
      mockParent.observed(
        any[CantonTimestamp],
        any[CantonTimestamp],
        any[SequencerCounter],
        anySeq[GenericSignedTopologyTransactionX],
      )(any[TraceContext])
    ).thenReturn(FutureUnlessShutdown.unit)

  }

  "caching client" should {
    import Fixture.*

    "return correct snapshot" in {

      for {
        _ <- cc
          .observed(ts1, ts1, SequencerCounter(1), Seq(mockTransaction))
          .failOnShutdown(s"at ${ts1}") // nonempty
        sp0a <- cc.snapshot(ts0)
        sp0b <- cc.snapshot(ts1)
        _ = cc.observed(ts1.plusSeconds(10), ts1.plusSeconds(10), SequencerCounter(1), Seq())
        keys0a <- sp0a.allKeys(owner)
        keys0b <- sp0b.allKeys(owner)
        keys1a <- cc.snapshot(ts1.plusSeconds(5)).flatMap(_.allKeys(owner))
        _ = cc.observed(ts2, ts2, SequencerCounter(1), Seq(mockTransaction))
        keys1b <- cc.snapshot(ts2).flatMap(_.allKeys(owner))
        _ = cc.observed(ts3, ts3, SequencerCounter(1), Seq())
        keys2a <- cc.snapshot(ts2.plusSeconds(5)).flatMap(_.allKeys(owner))
        keys2b <- cc.snapshot(ts3).flatMap(_.allKeys(owner))
      } yield {
        keys0a.signingKeys shouldBe Seq(key1)
        keys0b.signingKeys shouldBe Seq(key1)
        keys1a.signingKeys shouldBe Seq(key1, key2)
        keys1b.signingKeys shouldBe Seq(key1, key2)
        keys2a.signingKeys shouldBe Seq(key2)
        keys2b.signingKeys shouldBe Seq(key2)
      }

    }

    "verify we have properly cached our values" in {
      Future {
        verify(mockSnapshot0, times(2)).allKeys(owner)
        verify(mockSnapshot1, times(1)).allKeys(owner)
        verify(mockSnapshot2, times(1)).allKeys(owner)
        assert(true)
      }
    }

  }
}
