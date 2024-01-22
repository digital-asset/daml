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
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TopologyTransactionProcessorXTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

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
  ): Future[List[TopologyMappingX]] = {
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
  }

  private def process(
      proc: TopologyTransactionProcessorX,
      ts: CantonTimestamp,
      sc: Long,
      txs: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  ): Future[Unit] =
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
      val block1Adds = List(ns1k1_k1, ns1k2_k1, okm1bk5_k1, dtcp1_k1)
      val block1Replaces = List(dmp1_k1)
      val block2 = List(ns1k1_k1, setSerial(dmp1_k1_bis, PositiveInt.two))
      for {

        _ <- process(proc, ts(0), 0, block1Adds ++ block1Replaces)
        st0 <- fetch(store, ts(0).immediateSuccessor)
        _ <- process(proc, ts(1), 1, block2)
        st1 <- fetch(store, ts(1).immediateSuccessor)
        // finds the most recently stored version of a transaction, including rejected ones
        rejected_ns1k1_k1O <- store.findStored(ns1k1_k1, includeRejected = true)

      } yield {
        val rejected_ns1k1_k1 =
          rejected_ns1k1_k1O.valueOrFail("Unable to find ns1k1_k1 in the topology store")
        // the rejected ns1k1_k1 should not be valid
        rejected_ns1k1_k1.validUntil shouldBe Some(rejected_ns1k1_k1.validFrom)

        validate(st0, block1Adds ++ block1Replaces)
        validate(st1, block1Adds :+ dmp1_k1_bis)

      }
    }

    "deal with incremental additions" in {
      val (proc, store) = mk()
      val block1Adds = List(ns1k1_k1, ns1k2_k1)
      val block1Replaces = List(dmp1_k1)
      val block1 = block1Adds ++ block1Replaces
      val block2 = List(okm1bk5_k1, dtcp1_k1, setSerial(dmp1_k1_bis, PositiveInt.two))
      for {
        _ <- process(proc, ts(0), 0, block1)
        st0 <- fetch(store, ts(0).immediateSuccessor)
        _ <- process(proc, ts(1), 1, block2)
        st1 <- fetch(store, ts(1).immediateSuccessor)

      } yield {
        validate(st0, block1)
        validate(st1, block1Adds ++ block2) // dmp1_k1_bis replaces dmp1_k1
      }
    }

    "deal with removals" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1)
      val block2 = block1.reverse.map(Factory.mkRemoveTx)
      for {
        _ <- process(proc, ts(0), 0, block1)
        _ <- process(proc, ts(1), 1, block2)
        st1 <- fetch(store, ts(0).immediateSuccessor)
        st2 <- fetch(store, ts(1).immediateSuccessor)
      } yield {
        validate(st1, block1)
        st2 shouldBe empty
      }
    }

    "idempotent / crash recovery" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, dtcp1_k1)
      val block2 = List(okm1bk5_k1)
      for {
        _ <- process(proc, ts(0), 0, block1)
        _ <- process(proc, ts(1), 1, block2)
        proc2 = mk(store)._1
        _ <- process(proc2, ts(0), 0, block1)
        _ <- process(proc2, ts(1), 1, block2)
        st1 <- fetch(store, ts(0).immediateSuccessor)
        st2 <- fetch(store, ts(1).immediateSuccessor)
      } yield {
        validate(st1, block1)
        validate(st2, block1 ++ block2)
      }
    }

    // TODO(#12390) enable test after support for cascading updates
    "cascading update" ignore {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5_k4)
      for {
        _ <- process(proc, ts(0), 0, block1)
        st1 <- fetch(store, ts(0).immediateSuccessor)
        _ <- process(proc, ts(1), 1, List(Factory.mkRemoveTx(ns1k2_k1)))
        st2 <- fetch(store, ts(1).immediateSuccessor)
        _ <- process(proc, ts(2), 2, List(setSerial(ns1k2_k1p, PositiveInt.tryCreate(3))))
        st3 <- fetch(store, ts(2).immediateSuccessor)
        _ <- process(proc, ts(3), 3, List(Factory.mkRemoveTx(id1ak4_k2)))
        st4 <- fetch(store, ts(3).immediateSuccessor)
        _ <- process(proc, ts(4), 4, List(setSerial(id1ak4_k2p, PositiveInt.tryCreate(3))))
        st5 <- fetch(store, ts(4).immediateSuccessor)
      } yield {
        validate(st1, block1)
        validate(st2, List(ns1k1_k1))
        validate(st3, List(ns1k1_k1, ns1k2_k1p, id1ak4_k2, okm1bk5_k4))
        validate(st4, List(ns1k1_k1, ns1k2_k1p))
        validate(st5, List(ns1k1_k1, ns1k2_k1p, id1ak4_k2p, okm1bk5_k4))
      }
    }

    "cascading update and domain parameters change" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, dmp1_k2)
      for {
        _ <- process(proc, ts(0), 0, block1)
        st1 <- fetch(store, ts(0).immediateSuccessor)
        _ <- process(proc, ts(1), 1, List(Factory.mkRemoveTx(ns1k2_k1)))
        st2 <- fetch(store, ts(1).immediateSuccessor)
      } yield {
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
    }

    "fetch previous authorizations" in {
      // after a restart, we need to fetch pre-existing authorizations from our store
      // simulate this one by one
      val store = mkStore
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5_k4)
      val appliedF = block1.zipWithIndex.foldLeft(Future.unit) { case (acc, (elem, idx)) =>
        val proc = mk(store)._1
        acc.flatMap { _ =>
          process(proc, ts(idx), idx.toLong, List(elem)).map(_ => ())
        }
      }
      for {
        _ <- appliedF
        st <- fetch(store, ts(3).immediateSuccessor)
      } yield {
        validate(st, block1)
      }

    }

  }

}
