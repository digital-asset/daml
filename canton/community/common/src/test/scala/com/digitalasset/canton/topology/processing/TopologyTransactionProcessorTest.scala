// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.Broadcast
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.topology.store.db.DbTopologyStoreXHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  Identifier,
  UniqueIdentifier,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

abstract class TopologyTransactionProcessorXTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private val crypto = new SymbolicPureCrypto()

  protected def mkStore: TopologyStoreX[TopologyStoreId.DomainStore]

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
      val rejected_ns1k1_k1O =
        store.findStored(CantonTimestamp.MaxValue, ns1k1_k1, includeRejected = true).futureValue
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
      val block1 = List(ns1k1_k1, dmp1_k1, ns2k2_k2, ns3k3_k3)
      val block2 = List(ns1k2_k1, dtcp1_k1)
      val block3 = List(okm1bk5_k1)
      val block4 = List(dnd_proposal_k1)
      val block5 = List(dnd_proposal_k2)
      val block6 = List(dnd_proposal_k3)
      process(proc, ts(0), 0, block1)
      process(proc, ts(1), 1, block2)
      process(proc, ts(2), 2, block3)
      process(proc, ts(3), 3, block4)
      process(proc, ts(4), 4, block5)
      process(proc, ts(5), 5, block6)
      val storeAfterProcessing = store.dumpStoreContent().futureValue
      val DNDafterProcessing = fetch(store, ts(5).immediateSuccessor)
        .find(_.code == TopologyMappingX.Code.DecentralizedNamespaceDefinitionX)
        .valueOrFail("Couldn't find DND")

      val proc2 = mk(store)._1
      process(proc2, ts(0), 0, block1)
      process(proc2, ts(1), 1, block2)
      process(proc2, ts(2), 2, block3)
      process(proc2, ts(3), 3, block4)
      process(proc2, ts(4), 4, block5)
      process(proc2, ts(5), 5, block6)
      val storeAfterReplay = store.dumpStoreContent().futureValue

      storeAfterReplay.result.size shouldBe storeAfterProcessing.result.size
      storeAfterReplay.result.zip(storeAfterProcessing.result).foreach {
        case (replayed, original) => replayed shouldBe original
      }
      DNDafterProcessing shouldBe dnd_proposal_k1.mapping
      DNDafterProcessing shouldBe dnd_proposal_k1.mapping
    }

    // TODO(#12390) enable test after support for cascading updates
    "cascading update" ignore {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5_k4)
      process(proc, ts(0), 0, block1)
      val st1 = fetch(store, ts(0).immediateSuccessor)
      process(proc, ts(1), 1, List(Factory.mkRemoveTx(ns1k2_k1)))
      val st2 = fetch(store, ts(1).immediateSuccessor)
      process(proc, ts(2), 2, List(setSerial(ns1k2_k1p, PositiveInt.three)))
      val st3 = fetch(store, ts(2).immediateSuccessor)
      process(proc, ts(3), 3, List(Factory.mkRemoveTx(id1ak4_k2)))
      val st4 = fetch(store, ts(3).immediateSuccessor)
      process(proc, ts(4), 4, List(setSerial(id1ak4_k2p, PositiveInt.three)))
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

    "correctly handle duplicate transactions within the same batch" in {
      import SigningKeys.{ec as _, *}
      val dnsNamespace =
        DecentralizedNamespaceDefinitionX.computeNamespace(Set(ns1, ns7, ns8, ns9))
      val domainId = DomainId(UniqueIdentifier(Identifier.tryCreate("test-domain"), dnsNamespace))

      val dns = mkAddMultiKey(
        DecentralizedNamespaceDefinitionX
          .create(
            dnsNamespace,
            PositiveInt.three,
            NonEmpty(Set, ns1, ns7, ns8, ns9),
          )
          .value,
        NonEmpty(Set, key1, key7, key8, key9),
      )

      val mdsMapping = MediatorDomainStateX
        .create(
          domainId,
          NonNegativeInt.zero,
          PositiveInt.one,
          active = Seq(DefaultTestIdentities.mediatorIdX),
          observers = Seq.empty,
        )
        .value
      val mds = mkAddMultiKey(
        mdsMapping,
        NonEmpty(Set, key1, key7, key8),
      )

      val (proc, store) = mk()

      def checkMds(
          ts: CantonTimestamp,
          expectedSignatures: Int,
          expectedValidFrom: CantonTimestamp,
      ) = {
        val mdsInStore = store
          .findStored(ts, mds, includeRejected = false)
          .futureValue
          .value

        mdsInStore.mapping shouldBe mdsMapping
        mdsInStore.transaction.signatures.forgetNE.toSeq should have size (expectedSignatures.toLong)
        mdsInStore.validUntil shouldBe None
        mdsInStore.validFrom shouldBe EffectiveTime(expectedValidFrom)
      }

      // setup
      val block0 = List[GenericSignedTopologyTransactionX](
        ns1k1_k1,
        ns7k7_k7,
        ns8k8_k8,
        ns9k9_k9,
        dns,
        mds,
      )

      process(proc, ts(0), 0L, block0)
      validate(fetch(store, ts(0).immediateSuccessor), block0)
      // check that the most recently stored version after ts(0) is the one with 3 signatures
      checkMds(ts(0).immediateSuccessor, expectedSignatures = 3, expectedValidFrom = ts(0))

      val extraMds = mkAdd(mdsMapping, signingKey = key9, isProposal = true)

      // processing multiple of the same transaction in the same batch works correctly
      val block1 = List[GenericSignedTopologyTransactionX](extraMds, extraMds)
      process(proc, ts(1), 1L, block1)
      validate(fetch(store, ts(1).immediateSuccessor), block0)
      // check that the most recently stored version after ts(1) is the merge of the previous one with the additional signature
      // for a total of 4 signatures
      checkMds(ts(1).immediateSuccessor, expectedSignatures = 4, expectedValidFrom = ts(1))

      // processing yet another instance of the same transaction out of batch will result in a rejected
      // transaction
      val block2 = List(extraMds)
      process(proc, ts(2), 2L, block2)
      validate(fetch(store, ts(2).immediateSuccessor), block0)

      // look up the most recently stored mds transaction (including rejected ones). since we just processed a duplicate,
      // we expect to get back that duplicate. We cannot check for rejection reasons directly (they are pretty much write-only),
      // but we can check that it was immediately invalidated (validFrom == validUntil) which happens for rejected transactions.
      val rejectedMdsInStoreAtTs2 = store
        .findStored(ts(2).immediateSuccessor, extraMds, includeRejected = true)
        .futureValue
        .value
      rejectedMdsInStoreAtTs2.validFrom shouldBe rejectedMdsInStoreAtTs2.validUntil.value
      rejectedMdsInStoreAtTs2.validFrom shouldBe EffectiveTime(ts(2))

      // the latest non-rejected transaction should still be the one
      checkMds(ts(2).immediateSuccessor, expectedSignatures = 4, expectedValidFrom = ts(1))
    }

  }

}

class TopologyTransactionProcessorXTestInMemory extends TopologyTransactionProcessorXTest {
  protected def mkStore: TopologyStoreX[TopologyStoreId.DomainStore] =
    new InMemoryTopologyStoreX(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
    )

}
class TopologyTransactionProcessorXTestPostgres
    extends TopologyTransactionProcessorXTest
    with DbTest
    with DbTopologyStoreXHelper
    with PostgresTest {
  override protected def mkStore: TopologyStoreX[TopologyStoreId.DomainStore] =
    createTopologyStore()
}
