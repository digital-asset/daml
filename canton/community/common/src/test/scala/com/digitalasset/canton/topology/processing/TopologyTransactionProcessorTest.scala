// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.Broadcast
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

class TopologyTransactionProcessorXTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val crypto = new SymbolicPureCrypto()

  private def mkStore: InMemoryTopologyStoreX[TopologyStoreId.DomainStore] =
    new InMemoryTopologyStoreX(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
    )

  private def mk(
      store: TopologyStoreX[TopologyStoreId.DomainStore] = mkStore
  ): (TopologyTransactionProcessorX, TopologyStoreX[TopologyStoreId.DomainStore]) = {

    val proc = new TopologyTransactionProcessorX(
      DefaultTestIdentities.domainId,
      crypto,
      store,
      _ => (),
      TerminateProcessing.NoOpTerminateTopologyProcessing,
      true,
      futureSupervisor,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    (proc, store)
  }

  private def ts(idx: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(idx.toLong)
  private def fetch(
      store: TopologyStoreX[TopologyStoreId],
      timestamp: CantonTimestamp,
  ): List[TopologyMappingX] = {
    store
      .findPositiveTransactions(
        asOf = timestamp,
        asOfInclusive = false,
        isProposal = false,
        types = TopologyMappingX.Code.all,
        None,
        None,
      )
      .map(_.toTopologyState)
      .futureValue
  }

  private def process(
      proc: TopologyTransactionProcessorX,
      ts: CantonTimestamp,
      sc: Long,
      txs: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  ): Unit =
    clue(s"block at sc $sc")(
      proc
        .process(
          SequencedTime(ts),
          EffectiveTime(ts),
          SequencerCounter(sc),
          List(
            TopologyTransactionsBroadcastX.create(
              DefaultTestIdentities.domainId,
              Seq(Broadcast(String255.tryCreate("some request"), txs)),
              testedProtocolVersion,
            )
          ),
        )
        .onShutdown(fail())
        .futureValue
    )

  private def validate(
      observed: List[TopologyMappingX],
      expected: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  ) = {
    observed.toSet shouldBe expected.map(_.mapping).toSet
  }

  object Factory extends TopologyTransactionTestFactoryX(loggerFactory, parallelExecutionContext)
  import Factory.*

  "topology transaction processor" should {
    "deal with additions" in {
      val (proc, store) = mk()
      // topology processor assumes to be able to find domain parameters in the store for additional checks
      val block1 = List(ns1k1_k1, dmp1_k1)
      val block2Adds = List(ns1k2_k1, okm1bk5_k1, dtcp1_k1)
      val block3Replaces = List(ns1k1_k1, setSerial(dmp1_k1_bis, PositiveInt.two))

      process(proc, ts(0), 0, block1)
      process(proc, ts(1), 1, block2Adds)
      val st1 = fetch(store, ts(1).immediateSuccessor)
      process(proc, ts(2), 2, block3Replaces)
      val st2 = fetch(store, ts(2).immediateSuccessor)

      // finds the most recently stored version of a transaction, including rejected ones
      val rejected_ns1k1_k1O = store.findStored(ns1k1_k1, includeRejected = true).futureValue
      val rejected_ns1k1_k1 =
        rejected_ns1k1_k1O.valueOrFail("Unable to find ns1k1_k1 in the topology store")
      // the rejected ns1k1_k1 should not be valid
      rejected_ns1k1_k1.validUntil shouldBe Some(rejected_ns1k1_k1.validFrom)

      validate(st1, block1 ++ block2Adds)
      validate(st2, ns1k1_k1 +: block2Adds :+ dmp1_k1_bis)
    }

    "deal with incremental additions" in {
      val (proc, store) = mk()
      val block1Adds = List(ns1k1_k1, ns1k2_k1)
      val block1Replaces = List(dmp1_k1)
      val block1 = block1Adds ++ block1Replaces
      val block2 = List(okm1bk5_k1, dtcp1_k1, setSerial(dmp1_k1_bis, PositiveInt.two))

      process(proc, ts(0), 0, block1)
      val st0 = fetch(store, ts(0).immediateSuccessor)
      process(proc, ts(1), 1, block2)
      val st1 = fetch(store, ts(1).immediateSuccessor)

      validate(st0, block1)
      validate(st1, block1Adds ++ block2) // dmp1_k1_bis replaces dmp1_k1
    }

    "deal with removals" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1)
      val block2 = block1.reverse.map(Factory.mkRemoveTx)
      process(proc, ts(0), 0, block1)
      process(proc, ts(1), 1, block2)
      val st1 = fetch(store, ts(0).immediateSuccessor)
      val st2 = fetch(store, ts(1).immediateSuccessor)

      validate(st1, block1)
      st2 shouldBe empty
    }

    "idempotent / crash recovery" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, dmp1_k1)
      val block2 = List(ns1k2_k1, dtcp1_k1)
      val block3 = List(okm1bk5_k1)
      process(proc, ts(0), 0, block1)
      process(proc, ts(1), 1, block2)
      process(proc, ts(2), 2, block3)
      val proc2 = mk(store)._1
      process(proc2, ts(0), 0, block1)
      process(proc2, ts(1), 1, block2)
      process(proc2, ts(2), 2, block3)
      val st2 = fetch(store, ts(1).immediateSuccessor)
      val st3 = fetch(store, ts(2).immediateSuccessor)

      validate(st2, block1 ++ block2)
      validate(st3, block1 ++ block2 ++ block3)
    }

    // TODO(#12390) enable test after support for cascading updates
    "cascading update" ignore {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5_k4)
      process(proc, ts(0), 0, block1)
      val st1 = fetch(store, ts(0).immediateSuccessor)
      process(proc, ts(1), 1, List(Factory.mkRemoveTx(ns1k2_k1)))
      val st2 = fetch(store, ts(1).immediateSuccessor)
      process(proc, ts(2), 2, List(setSerial(ns1k2_k1p, PositiveInt.tryCreate(3))))
      val st3 = fetch(store, ts(2).immediateSuccessor)
      process(proc, ts(3), 3, List(Factory.mkRemoveTx(id1ak4_k2)))
      val st4 = fetch(store, ts(3).immediateSuccessor)
      process(proc, ts(4), 4, List(setSerial(id1ak4_k2p, PositiveInt.tryCreate(3))))
      val st5 = fetch(store, ts(4).immediateSuccessor)
      validate(st1, block1)
      validate(st2, List(ns1k1_k1))
      validate(st3, List(ns1k1_k1, ns1k2_k1p, id1ak4_k2, okm1bk5_k4))
      validate(st4, List(ns1k1_k1, ns1k2_k1p))
      validate(st5, List(ns1k1_k1, ns1k2_k1p, id1ak4_k2p, okm1bk5_k4))
    }

    "cascading update and domain parameters change" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, dmp1_k2)
      process(proc, ts(0), 0, block1)
      val st1 = fetch(store, ts(0).immediateSuccessor)
      process(proc, ts(1), 1, List(Factory.mkRemoveTx(ns1k2_k1)))
      val st2 = fetch(store, ts(1).immediateSuccessor)
      validate(st1, block1)

      /*
          dmp1_k2 is not revoked
          Domain governance transaction are not removed by cascading updates. The
          idea behind is that the change of domain parameters is authorized and then
          the new parameters stay valid even if the authorizing key is revoked. That
          also ensures that we always have some domain parameters set.
       */
      validate(st2, List(ns1k1_k1, dmp1_k2))
    }

    "fetch previous authorizations" in {
      // after a restart, we need to fetch pre-existing authorizations from our store
      // simulate this one by one
      val store = mkStore
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5_k4)
      block1.zipWithIndex.foreach { case (elem, idx) =>
        val proc = mk(store)._1
        process(proc, ts(idx), idx.toLong, List(elem))
      }
      val st = fetch(store, ts(3).immediateSuccessor)
      validate(st, block1)

    }

  }

}
