// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TopologyTransactionProcessorTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  private val crypto = new SymbolicPureCrypto()

  private def mkStore: InMemoryTopologyStore[TopologyStoreId.DomainStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
      futureSupervisor,
    )

  private def mk(
      store: TopologyStore[TopologyStoreId.DomainStore] = mkStore
  ): (TopologyTransactionProcessor, TopologyStore[TopologyStoreId.DomainStore]) = {

    val proc = new TopologyTransactionProcessor(
      DefaultTestIdentities.domainId,
      DomainTopologyTransactionMessageValidator.NoValidation,
      crypto,
      store,
      _ => (),
      FutureSupervisor.Noop,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    (proc, store)
  }

  private def ts(idx: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(idx.toLong)
  private def fetch(
      store: TopologyStore[TopologyStoreId],
      timestamp: CantonTimestamp,
  ): Future[List[TopologyStateElement[TopologyMapping]]] = {
    store
      .findStateTransactions(
        asOf = timestamp,
        asOfInclusive = false,
        includeSecondary = false,
        DomainTopologyTransactionType.all,
        None,
        None,
      )
      .map(_.toIdentityState)
  }

  private def process(
      proc: TopologyTransactionProcessor,
      ts: CantonTimestamp,
      sc: Long,
      txs: List[SignedTopologyTransaction[TopologyChangeOp]],
  ): Future[Unit] =
    proc.process(SequencedTime(ts), EffectiveTime(ts), SequencerCounter(sc), txs).onShutdown(fail())

  private def validate(
      observed: List[TopologyStateElement[TopologyMapping]],
      expected: List[SignedTopologyTransaction[TopologyChangeOp]],
  ) = {
    val mp1 = observed.map(_.mapping).toSet
    val mp2 = expected.map(_.transaction.element.mapping).toSet
    mp1 shouldBe mp2
    observed.toSet shouldBe expected.map(_.transaction.element).toSet
  }

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)
  import Factory.*

  "topology transaction processor" should {
    "deal with additions" in {
      val (proc, store) = mk()
      val block1Adds = List(ns1k1_k1, ns1k2_k1, okm1bk5_k1, p1p1B_k2)
      val block1Replaces = List(dmp1_k1)
      val block2 = List(ns1k1_k1, dmp1_k1_bis)
      for {

        _ <- process(proc, ts(0), 0, block1Adds ++ block1Replaces)
        st0 <- fetch(store, ts(0).immediateSuccessor)
        _ <- loggerFactory.assertLogs(
          process(proc, ts(1), 1, block2),
          _.warningMessage should include("Duplicate"),
        )
        st1 <- fetch(store, ts(1).immediateSuccessor)

      } yield {
        validate(st0, block1Adds ++ block1Replaces)
        validate(st1, block1Adds :+ dmp1_k1_bis)
      }
    }

    "deal with incremental additions" in {
      val (proc, store) = mk()
      val block1Adds = List(ns1k1_k1, ns1k2_k1)
      val block1Replaces = List(dmp1_k1)
      val block1 = block1Adds ++ block1Replaces
      val block2 = List(okm1bk5_k1, p1p1B_k2, dmp1_k1_bis)
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
      val block2 = block1.reverse.map(Factory.revert)
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
      val block1 = List(ns1k1_k1, ns1k2_k1, okm1bk5_k1, p1p1B_k2)
      val block2 = List(p1p2F_k2)
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

    "cascading update" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5_k4)
      for {
        _ <- process(proc, ts(0), 0, block1)
        st1 <- fetch(store, ts(0).immediateSuccessor)
        _ <- process(proc, ts(1), 1, List(Factory.revert(ns1k2_k1)))
        st2 <- fetch(store, ts(1).immediateSuccessor)
        _ <- process(proc, ts(2), 2, List(ns1k2_k1p))
        st3 <- fetch(store, ts(2).immediateSuccessor)
        _ <- process(proc, ts(3), 3, List(Factory.revert(id1ak4_k2)))
        st4 <- fetch(store, ts(3).immediateSuccessor)
        _ <- process(proc, ts(4), 4, List(id1ak4_k2p))
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
        _ <- process(proc, ts(1), 1, List(Factory.revert(ns1k2_k1)))
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
