// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.cache

import cats.syntax.option.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache.{
  MaybeUpdatedTx,
  StateData,
  StateKey,
}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.Assertion

import scala.language.implicitConversions

class StateDataTest extends BaseTestWordSpec with HasExecutionContext {

  private object Factory
      extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext) {}
  import Factory.*
  private lazy val sk = StateKey(ns1k1_k1.mapping)

  private implicit def toEffectiveTime(ts: CantonTimestamp): EffectiveTime = EffectiveTime(ts)
  private implicit def toSequencedTime(ts: CantonTimestamp): SequencedTime = SequencedTime(ts)
  private def ts(off: Int): CantonTimestamp = CantonTimestamp.Epoch.plusMillis(off.toLong)

  private def toStored(
      tx: GenericSignedTopologyTransaction,
      from: CantonTimestamp,
      until: Option[CantonTimestamp],
      rejection: Option[String] = None,
  ): GenericStoredTopologyTransaction =
    StoredTopologyTransaction(
      from,
      from,
      until.map(EffectiveTime(_)),
      tx,
      rejection.map(String300.tryCreate(_)),
    )

  private object Txs {
    val ns1k1_k1_0_N = toStored(ns1k1_k1, ts(0), None)
    val ns1k1_k1_1_1_r =
      toStored(ns1k1_k1_unsupportedScheme, ts(1), ts(1).some, Some("Not supported"))
    val ns1k2_k1_1_3 = toStored(ns1k2_k1, ts(1), ts(3).some)
    val ns1k2_k1_1_3rm = toStored(mkRemoveTx(ns1k2_k1), ts(3), ts(3).some)
    val ns1k3_k2_2_N = toStored(ns1k3_k2, ts(3), None)
  }
  import Txs.*

  private def validate(
      have: Seq[GenericStoredTopologyTransaction],
      expect: Seq[GenericStoredTopologyTransaction],
  ): Assertion = {

    have should contain theSameElementsAs expect

    succeed
  }

  "state data" should {

    "add to head and move archived to tail" in {
      val s = StateData.fromLoaded(sk, Seq(ns1k1_k1_1_1_r), ts(-1))

      // we first add one
      val m = new MaybeUpdatedTx(sk, ns1k1_k1_0_N, false, None)
      val s2 = s.addNewTransaction(m)

      // we can find it now
      s2.filterState(ts(0), asOfInclusive = true, op = TopologyChangeOp.Replace) shouldBe Seq(
        ns1k1_k1_0_N
      )

      // we add a second one
      val ns1k2_k1_1_3p = ns1k2_k1_1_3.copy(validUntil = None)
      val m2 = new MaybeUpdatedTx(sk, ns1k2_k1_1_3p, false, None)
      val s3 = s2.addNewTransaction(m2)

      // we find both
      validate(
        s3.filterState(ts(0), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(
          ns1k1_k1_0_N
        ),
      )
      validate(
        s3.filterState(ts(1), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(
          ns1k2_k1_1_3p,
          ns1k1_k1_0_N,
        ),
      )
      validate(
        s3.filterState(ts(1), asOfInclusive = false, op = TopologyChangeOp.Replace),
        Seq(
          ns1k1_k1_0_N
        ),
      )

      // we add the removal of m2
      val m3 = new MaybeUpdatedTx(sk, ns1k2_k1_1_3rm, false, None)
      m2.expireAt(ts(3))

      // s3 should see consistent results despite m2 being archived
      validate(
        s3.filterState(ts(1), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(
          ns1k1_k1_0_N,
          ns1k2_k1_1_3,
        ),
      )
      validate(
        s3.filterState(ts(3), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(
          ns1k1_k1_0_N
        ),
      )
      validate(
        s3.filterState(ts(3), asOfInclusive = false, op = TopologyChangeOp.Replace),
        Seq(
          ns1k1_k1_0_N,
          ns1k2_k1_1_3,
        ),
      )

      val s4 = s3.addNewTransaction(m3)

      validate(
        s3.filterState(ts(1), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(
          ns1k2_k1_1_3,
          ns1k1_k1_0_N,
        ),
      )
      validate(
        s4.filterState(ts(1), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(
          ns1k1_k1_0_N,
          ns1k2_k1_1_3,
        ),
      )
      // now test with items being in head and in tail
      val s5 = s4.transferArchivedToTail(true)
      Seq(s4, s5).foreach { ss =>
        // TODO(#29400) removed is not really state so doesn't show up here
        validate(ss.filterState(ts(3), asOfInclusive = true, op = TopologyChangeOp.Remove), Seq())
        // find correct txs
        validate(
          ss.filterState(ts(3), asOfInclusive = true, op = TopologyChangeOp.Replace),
          Seq(
            ns1k1_k1_0_N
          ),
        )
        validate(
          ss.filterState(ts(3), asOfInclusive = false, op = TopologyChangeOp.Replace),
          Seq(
            ns1k1_k1_0_N,
            ns1k2_k1_1_3,
          ),
        )
        validate(
          ss.filterState(ts(1), asOfInclusive = true, op = TopologyChangeOp.Replace),
          Seq(
            ns1k1_k1_0_N,
            ns1k2_k1_1_3,
          ),
        )
        validate(
          ss.filterState(ts(1), asOfInclusive = false, op = TopologyChangeOp.Replace),
          Seq(
            ns1k1_k1_0_N
          ),
        )
      }
    }

    "drop pending changes and always find correct head state" in {

      val ns1k2_k1_1_3p = ns1k2_k1_1_3.copy(validUntil = None)
      val s = StateData.fromLoaded(sk, Seq(ns1k1_k1_0_N, ns1k2_k1_1_3), ts(1))
      validate(s.head.map(_.stored), Seq(ns1k1_k1_0_N))
      validate(s.tail, Seq(ns1k2_k1_1_3))
      s.findAtMostOne(_.mapping == ns1k1_k1_0_N.mapping, true, "test") should not be empty

      val s1 = s.dropPendingChanges(ts(2))
      validate(s1.head.map(_.stored), Seq(ns1k1_k1_0_N, ns1k2_k1_1_3p))
      s1.findAtMostOne(_.mapping == ns1k1_k1_0_N.mapping, true, "test")
        .map(_.stored) should contain(ns1k1_k1_0_N)
      s1.findAtMostOne(_.mapping == ns1k2_k1_1_3.mapping, true, "test")
        .map(_.stored) should contain(ns1k2_k1_1_3p)
      s1.findLastUpdateForMapping(true, ns1k1_k1_0_N.mapping) should contain(ns1k1_k1_0_N)
      s1.findLastUpdateForMapping(true, ns1k2_k1_1_3p.mapping) should contain(ns1k2_k1_1_3p)
      s1.tail shouldBe empty

      val s2 = s.dropPendingChanges(ts(3))
      validate(s2.head.map(_.stored), Seq(ns1k1_k1_0_N, ns1k2_k1_1_3p))
      s2.tail shouldBe empty
      s2.findLastUpdateForMapping(true, ns1k1_k1_0_N.mapping) should contain(ns1k1_k1_0_N)
      s2.findLastUpdateForMapping(true, ns1k2_k1_1_3p.mapping) should contain(ns1k2_k1_1_3p)

      val s3 = s.dropPendingChanges(ts(1))
      validate(s3.head.map(_.stored), Seq(ns1k1_k1_0_N))
      s3.tail shouldBe empty
      s3.findLastUpdateForMapping(true, ns1k1_k1_0_N.mapping) should contain(ns1k1_k1_0_N)
      s3.findLastUpdateForMapping(true, ns1k2_k1_1_3p.mapping) shouldBe empty

    }

    "deal with historical" in {

      val s = StateData.fromLoaded(sk, Seq(ns1k1_k1_0_N), ts(4))
      val sh =
        s.updateHistorical(
          EffectiveTime(CantonTimestamp.MinValue),
          Seq(ns1k1_k1_0_N, ns1k2_k1_1_3),
          enableConsistencyChecks = true,
        )

      validate(
        sh.filterState(ts(2), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(ns1k1_k1_0_N, ns1k2_k1_1_3),
      )
      validate(
        sh.filterState(ts(0), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(ns1k1_k1_0_N),
      )
      validate(
        sh.filterState(ts(3), asOfInclusive = true, op = TopologyChangeOp.Replace),
        Seq(ns1k1_k1_0_N),
      )
      validate(
        sh.filterState(ts(3), asOfInclusive = false, op = TopologyChangeOp.Replace),
        Seq(ns1k1_k1_0_N, ns1k2_k1_1_3),
      )

      // throws if we access out of bounds
      loggerFactory.assertThrowsAndLogs[IllegalStateException](
        s.filterState(ts(2), asOfInclusive = true, op = TopologyChangeOp.Replace),
        _.errorMessage should include("An internal error has occurred"),
      )

    }

  }
}
