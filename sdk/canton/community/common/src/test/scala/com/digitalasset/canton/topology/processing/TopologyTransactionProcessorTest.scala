// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  DefaultProcessingTimeouts,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.{SigningPublicKey, SynchronizerCryptoPureApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.cache.{CacheTestMetrics, TopologyStateWriteThroughCache}
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{FailOnShutdown, SequencerCounter}
import org.scalatest.Assertion

abstract class TopologyTransactionProcessorTest
    extends TopologyTransactionHandlingBase
    with FailOnShutdown {

  import Factory.*

  protected def mkDefault(
      testName: String = "processortest"
  ): (TopologyTransactionProcessor, TopologyStore[TopologyStoreId.SynchronizerStore]) = mk(
    mkStore(
      Factory.physicalSynchronizerId1a,
      testName,
    )
  )

  protected def mk(
      store: TopologyStore[TopologyStoreId.SynchronizerStore]
  ): (TopologyTransactionProcessor, TopologyStore[TopologyStoreId.SynchronizerStore]) = {

    val proc = new TopologyTransactionProcessor(
      new SynchronizerCryptoPureApi(defaultStaticSynchronizerParameters, crypto),
      store,
      new TopologyStateWriteThroughCache(
        store,
        BatchAggregatorConfig.defaultsForTesting,
        cacheEvictionThreshold = TopologyConfig.forTesting.topologyStateCacheEvictionThreshold,
        maxCacheSize = TopologyConfig.forTesting.maxTopologyStateCacheItems,
        enableConsistencyChecks = true,
        CacheTestMetrics.metrics,
        futureSupervisor,
        timeouts,
        loggerFactory,
      ),
      defaultStaticSynchronizerParameters,
      _ => (),
      TerminateProcessing.NoOpTerminateTopologyProcessing,
      futureSupervisor,
      exitOnFatalFailures = true,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    (proc, store)
  }

  protected def process(
      proc: TopologyTransactionProcessor,
      ts: CantonTimestamp,
      sc: Long,
      txs: List[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): Unit =
    clue(s"block at sc $sc")(
      proc
        .process(
          SequencedTime(ts),
          EffectiveTime(ts),
          SequencerCounter(sc),
          txs,
        )
        .onShutdown(fail())
        .futureValue
    )

  "topology transaction processor" when {
    "processing transactions from a synchronizer" should {
      "deal with additions" in {
        val (proc, store) = mkDefault()
        // topology processor assumes to be able to find synchronizer parameters in the store for additional checks
        val block1 = List(ns1k1_k1, dmp1_k1)
        val block2Adds = List(ns1k2_k1, okm1bk5k1E_k1, dtcp1_k1)
        val block3Replaces = List(ns1k8_k3_fail, ns1k1_k1, setSerial(dmp1_k1_bis, PositiveInt.two))

        process(proc, ts(0), 0, block1)
        process(proc, ts(1), 1, block2Adds)
        val st1 = fetch(store, ts(1).immediateSuccessor)
        process(proc, ts(2), 2, block3Replaces)
        val st2 = fetch(store, ts(2).immediateSuccessor)

        // finds the most recently stored version of a transaction, including rejected ones
        val rejected_ns1k8_k3_fail =
          store
            .findStored(CantonTimestamp.MaxValue, ns1k8_k3_fail, includeRejected = true)
            .futureValueUS
            .valueOrFail("Unable to find ns1k8_k3_fail in the topology store")
        // the rejected ns1k1_k1 should not be valid
        rejected_ns1k8_k3_fail.validUntil shouldBe Some(rejected_ns1k8_k3_fail.validFrom)

        validate(st1, block1 ++ block2Adds)
        validate(st2, ns1k1_k1 +: block2Adds :+ dmp1_k1_bis)

      }

      "deal with incremental additions" in {
        val (proc, store) = mkDefault()
        val block1Adds = List(ns1k1_k1, ns1k2_k1)
        val block1Replaces = List(dmp1_k1)
        val block1 = block1Adds ++ block1Replaces
        val block2 = List(okm1bk5k1E_k1, dtcp1_k1, setSerial(dmp1_k1_bis, PositiveInt.two))

        process(proc, ts(0), 0, block1)
        val st0 = fetch(store, ts(0).immediateSuccessor)
        process(proc, ts(1), 1, block2)
        val st1 = fetch(store, ts(1).immediateSuccessor)

        validate(st0, block1)
        validate(st1, block1Adds ++ block2) // dmp1_k1_bis replaces dmp1_k1
      }

      "deal with removals" in {
        val (proc, store) = mkDefault()
        val block1 = List(ns1k1_k1, ns1k2_k1)
        val block2 = block1.reverse.map(Factory.mkRemoveTx)
        process(proc, ts(0), 0, block1)
        process(proc, ts(1), 1, block2)
        val st1 = fetch(store, ts(0).immediateSuccessor)
        val st2 = fetch(store, ts(1).immediateSuccessor)
        validate(st1, block1)
        st2 shouldBe empty
      }

      "deal with add and remove in the same block" in {
        val (proc, store) = mkDefault()
        process(proc, ts(0), 0, List(ns1k1_k1, okm1bk5k1E_k1, Factory.mkRemoveTx(okm1bk5k1E_k1)))
        val st1 = fetch(store, ts(0).immediateSuccessor)
        validate(st1, List(ns1k1_k1))
      }

      "deal with add, remove and readd" in {
        import SigningKeys.*
        val (proc, store) = mkDefault()

        val p1s =
          mkAdd(
            PartyToParticipant.tryCreate(
              party1b,
              threshold = PositiveInt.one,
              Seq(
                HostingParticipant(participant1, ParticipantPermission.Submission)
              ),
            ),
            key1,
          )
        val rm2 = Factory.mkRemoveTx(p1s)
        val p3s_fail =
          mkAdd(
            PartyToParticipant.tryCreate(
              party1b,
              threshold = PositiveInt.one,
              Seq(HostingParticipant(participant1, ParticipantPermission.Observation)),
            ),
            key1,
            serial = PositiveInt.one,
          )
        val p3s =
          mkAdd(
            PartyToParticipant.tryCreate(
              party1b,
              threshold = PositiveInt.one,
              Seq(HostingParticipant(participant1, ParticipantPermission.Observation)),
            ),
            key1,
            serial = PositiveInt.three,
          )
        process(proc, ts(0), 0, List(ns1k1_k1, dmp1_k1, okm1bk5k1E_k1, dtcp1_k1, p1s))
        // this will remove the p2p
        process(proc, ts(1), 1, List(rm2))

        // this should fail as the p2p has serial = 1, but we already added the remove with serial = 2
        process(proc, ts(2), 2, List(p3s_fail))
        validate(
          fetch(store, ts(0).immediateSuccessor),
          List(ns1k1_k1, dmp1_k1, okm1bk5k1E_k1, dtcp1_k1, p1s),
        )
        // we need to make sure that the prior remove remains the active one
        val ret =
          store.dumpStoreContent().futureValueUS.result.filter(_.mapping.code == p1s.mapping.code)
        ret.map(tx =>
          (tx.transaction.operation, tx.validUntil.map(_.value), tx.rejectionReason)
        ) shouldBe Seq(
          (TopologyChangeOp.Replace, Some(ts(1)), None),
          (TopologyChangeOp.Remove, None, None),
          (
            TopologyChangeOp.Replace,
            Some(ts(2)),
            Some("The actual serial 1 does not match the expected serial 3"),
          ),
        )

        // removal should have succeeded
        val st1 = fetch(store, ts(2).immediateSuccessor)
        validate(st1, List(ns1k1_k1, dmp1_k1, okm1bk5k1E_k1, dtcp1_k1))
        // good readditon works
        process(proc, ts(3), 3, List(p3s))
        validate(
          fetch(store, ts(3).immediateSuccessor),
          List(ns1k1_k1, dmp1_k1, okm1bk5k1E_k1, dtcp1_k1, p3s),
        )
        val (proc2, _) = mk(store)
        process(proc2, ts(2), 2, List(p3s_fail))
        process(proc2, ts(3), 3, List(p3s))
        validate(
          fetch(store, ts(3).immediateSuccessor),
          List(ns1k1_k1, dmp1_k1, okm1bk5k1E_k1, dtcp1_k1, p3s),
        )
        succeed
      }

      "idempotent / crash recovery" in {

        val (proc, store) = mkDefault("idempotent")
        val block1 = List(ns1k1_k1, dmp1_k1, ns2k2_k2, ns3k3_k3)
        val block2 = List(ns1k2_k1, okm1bk5k1E_k1)
        val block3 = List(dtcp1_k1)
        val block4 = List(dnd_proposal_k1)
        // using two competing proposals in the same block with the same
        // * serial
        // * mapping_unique_key
        // * operation
        // * validFrom
        // * signing keys
        // to check that the unique index is not too general
        val block5 = List(dnd_proposal_k2, dnd_proposal_k2_alternative)
        val block6 = List(dnd_proposal_k3)

        process(proc, ts(0), 0, block1)
        process(proc, ts(1), 1, block2)
        process(proc, ts(2), 2, block3)
        process(proc, ts(3), 3, block4)
        process(proc, ts(4), 4, block5)
        process(proc, ts(5), 5, block6)
        val storeAfterProcessing = store.dumpStoreContent().futureValueUS
        val DNDafterProcessing = fetch(store, ts(5).immediateSuccessor)
          .find(_.code == TopologyMapping.Code.DecentralizedNamespaceDefinition)
          .valueOrFail("Couldn't find DND")

        // check that we indeed stored 2 proposals
        storeAfterProcessing.result.filter(_.validFrom == EffectiveTime(ts(4))) should have size 2

        val proc2 = mk(store)._1
        process(proc2, ts(0), 0, block1)
        process(proc2, ts(1), 1, block2)
        process(proc2, ts(2), 2, block3)
        process(proc2, ts(3), 3, block4)
        process(proc2, ts(4), 4, block5)
        process(proc2, ts(5), 5, block6)
        val storeAfterReplay = store.dumpStoreContent().futureValueUS

        storeAfterReplay.result.size shouldBe storeAfterProcessing.result.size
        storeAfterReplay.result.zip(storeAfterProcessing.result).foreach {
          case (replayed, original) => replayed shouldBe original
        }
        DNDafterProcessing shouldBe dnd_proposal_k1.mapping
        DNDafterProcessing shouldBe dnd_proposal_k1.mapping
      }

      // the following suite of tests deals with out of band proposals and transactions
      // at the same time it tries out the same scenario in a variety of crash scenarios which all should
      // yield the same result
      "deal with out of band proposals and txs" should {
        def prepare(
            items: Seq[(TopologyTransactionProcessor, List[GenericSignedTopologyTransaction])],
            offset: Int = 0,
        ) =
          items.zipWithIndex.foreach { case ((proc, txs), idx) =>
            val idxWithOffset = idx + offset
            process(proc, ts(idxWithOffset), idxWithOffset.toLong, txs)
          }

        def mkPTP(
            serial: PositiveInt,
            isProposal: Boolean,
            signingKeys: NonEmpty[Set[SigningPublicKey]],
            permission: ParticipantPermission = ParticipantPermission.Submission,
        ) =
          mkAddMultiKey(
            PartyToParticipant.tryCreate(
              party6,
              threshold = PositiveInt.one,
              Seq(HostingParticipant(participant1, permission)),
            ),
            signingKeys = signingKeys,
            serial = serial,
            isProposal = isProposal,
          )

        def mkSetup() = {
          val (proc, store) = mkDefault()
          store.deleteAllData().futureValueUS
          // store first tx to increase test coverage (which now must make sure that initial loading works)
          store
            .update(
              SequencedTime(ts(-1)),
              EffectiveTime(ts(-1)),
              removals = Map(),
              additions = Seq(
                (ns1k1_k1, None),
                (dmp1_k1, None),
                (
                  p1p1B_k2,
                  Some(
                    TopologyTransactionRejection.Authorization.NoDelegationFoundForKeys(
                      Set(SigningKeys.key2.fingerprint)
                    )
                  ),
                ),
              ).map { case (tx, reason) =>
                ValidatedTopologyTransaction(tx, reason)
              },
            )
            .futureValueUS
          (proc, store, List(ns6k6_k6, ns1k3_k2, okm1bk5k1E_k1, dtcp1_k1))
        }

        def runScenarios(
            name: String,
            txs: List[GenericSignedTopologyTransaction],
            expectRejections: Seq[String] = Seq.empty,
        )(
            check: TopologyStore[TopologyStoreId] => Assertion
        ): Assertion = {
          def checkAndValidate(store: TopologyStore[TopologyStoreId]): Assertion = {
            val rejectedRaw = store
              .dumpStoreContent()
              .futureValueUS
              .result
              .filter(_.rejectionReason.nonEmpty)
            val garbage = Seq(p1p1B_k2, ns1k3_k2)
            val (expectedGarbage, rejected) =
              rejectedRaw.partition(c => garbage.contains(c.transaction))
            expectedGarbage should have length (2)
            forAll(rejected.zip(expectRejections)) { case (tx, reason) =>
              tx.rejectionReason.value.str should include(reason)
            }
            rejected should have length (expectRejections.length.toLong)
            check(store)
          }
          // run the same test in different scenarios where we crash at different points
          clue(name + " with all in one batch") {
            val (proc, store, base) = mkSetup()
            prepare(Seq((proc, base ++ txs)))
            checkAndValidate(store)
          }
          clue(name + " in separate batches") {
            val (proc, store, base) = mkSetup()
            prepare(Seq((proc, base)) ++ txs.map(tx => (proc, List(tx))))
            checkAndValidate(store)
          }
          clue(name + " in separate batches after crash") {
            val (proc, store, base) = mkSetup()
            val proc2 = mk(store)._1
            prepare(Seq((proc, base), (proc2, txs)))
            checkAndValidate(store)
          }
          clue(name + " in same batch with replay after crash") {
            val (proc, store, base) = mkSetup()
            val proc2 = mk(store)._1
            prepare(Seq((proc, base ++ txs)))
            prepare(Seq((proc2, base ++ txs)))
            checkAndValidate(store)
          }
          clue(name + " in different batches with replay after crash") {
            val (proc, store, base) = mkSetup()
            val proc2 = mk(store)._1
            prepare(Seq((proc, base), (proc, txs)))
            prepare(Seq((proc2, base), (proc2, txs)))
            checkAndValidate(store)
          }
          clue(name + "in different batches with partial replay after crash") {
            val (proc, store, base) = mkSetup()
            val proc2 = mk(store)._1
            prepare(Seq((proc, base), (proc, txs)))
            prepare(Seq((proc2, txs)), offset = 1)
            checkAndValidate(store)
          }
          succeed
        }

        "accept initial proposal with serial 2" in {
          val prop =
            mkPTP(
              serial = PositiveInt.two,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
            )
          runScenarios(
            "out-of-band proposal without any existing state",
            List(prop),
            if (this.testedProtocolVersion < ProtocolVersion.v35) List()
            else List("does not match the expected serial"),
          ) { store =>
            val txs = fetchTx(store, ts(10), isProposal = true).result.map(_.transaction)
            if (this.testedProtocolVersion < ProtocolVersion.v35) {
              txs shouldBe List(prop)
            } else {
              txs shouldBe empty
            }
          }
        }

        "accept initial tx with serial 2" in {
          val tx =
            mkPTP(
              serial = PositiveInt.two,
              isProposal = false,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
            )
          runScenarios("out-of-band initial tx without any existing state", List(tx)) { store =>
            fetch(store, ts(10)) should contain(tx.mapping)
          }
        }

        "reject update tx with gap between serials" in {
          val tx =
            mkPTP(
              serial = PositiveInt.two,
              isProposal = false,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
              permission = ParticipantPermission.Confirmation,
            )
          val tx2 =
            mkPTP(
              serial = PositiveInt.tryCreate(5),
              isProposal = false,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
            )
          runScenarios(
            "update tx with gap between serials",
            List(tx, tx2),
            Seq("The actual serial 5 does not match the expected serial 3"),
          ) { store =>
            // the second tx should bounce, so we should only find the first one
            fetch(store, ts(10)) should contain(tx.mapping)
          }
        }

        // proposal with gap between active
        "reject proposal with gap between serials" in {
          val tx =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = false,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
              permission = ParticipantPermission.Confirmation,
            )
          val tx2 =
            mkPTP(
              serial = PositiveInt.tryCreate(5),
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
            )
          runScenarios(
            "reject proposal with gap between serials",
            List(tx, tx2),
            Seq("The actual serial 5 does not match the expected serial 2"),
          ) { store =>
            // the second tx should bounce
            fetch(store, ts(10), isProposal = true) shouldBe empty
          }
        }

        // proposal with too low serial
        "reject proposal with too low serial" in {
          val tx =
            mkPTP(
              serial = PositiveInt.two,
              isProposal = false,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
              permission = ParticipantPermission.Confirmation,
            )
          val tx2 =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
            )
          runScenarios(
            "reject proposal with too low serial",
            List(tx, tx2),
            Seq("The actual serial 1 does not match the expected serial 3"),
          ) { store =>
            // the second tx should bounce
            fetch(store, ts(10), isProposal = true) shouldBe empty
          }
        }

        // proposal with conflicting serial
        "proposals with different serials" in {
          val tx =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
              permission = ParticipantPermission.Confirmation,
            )
          val tx2 =
            mkPTP(
              serial = PositiveInt.tryCreate(5),
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
            )
          runScenarios(
            "update tx with gap between serials",
            List(tx, tx2),
            if (testedProtocolVersion < ProtocolVersion.v35) List()
            else List("does not match the expected serial"),
          ) { store =>
            // the second tx should bounce
            validate(
              fetch(store, ts(10), isProposal = true),
              // older PVs allowed out of order proposals
              if (testedProtocolVersion < ProtocolVersion.v35)
                List(
                  tx,
                  tx2,
                )
              else List(tx),
            )
          }
        }

        "archiving proposals" in {
          val tx =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
              permission = ParticipantPermission.Confirmation,
            )
          val tx2 =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
            )
          val tx3 = // confirms tx
            mkTrans(
              tx.transaction,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key6),
              isProposal = true,
            )

          runScenarios("archive competing proposals", List(tx, tx2, tx3)) { store =>
            validate(
              fetch(store, ts(10), isProposal = true),
              List(),
            )
            fetch(store, ts(10)) should contain(tx.mapping)
          }
        }

        "competing but accumulating proposals" in {
          // here we will have three proposals that compete (tx1, tx1b, tx2)
          // tx2 has a different payload, tx1b should have different txHash but same payload
          val tx1 =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
              permission = ParticipantPermission.Confirmation,
            )
          // competes with tx1 subtly, as this is a new transaction
          // being created. the test tooling here will create a transaction
          // with some random bytes in the protobuf message such that we simulate
          // the situation where two transaction may have the same payload, but
          // different hash
          val tx1b =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
              permission = ParticipantPermission.Confirmation,
            )
          // competes with tx1
          val tx2 =
            mkPTP(
              serial = PositiveInt.one,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key1),
              permission = ParticipantPermission.Submission,
            )
          val tx3 = // confirms tx1
            mkTrans(
              tx1.transaction,
              isProposal = true,
              signingKeys = NonEmpty.mk(Set, SigningKeys.key6),
            )
          runScenarios("archive competing proposals", List(tx1, tx1b, tx2, tx3)) { store =>
            validate(
              fetch(store, ts(10), isProposal = true),
              List(),
            )
            fetch(store, ts(10)) should contain(tx1.mapping)
          }
        }

        "correct proposal aggregation and late signature aggregation" in {
          import SigningKeys.*
          val dndNamespace =
            DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns2, ns3))
          val dnd_s1_k1 =
            mkAdd(
              DecentralizedNamespaceDefinition.tryCreate(
                decentralizedNamespace = dndNamespace,
                owners = NonEmpty.mk(Set, ns1, ns2, ns3),
                threshold = PositiveInt.two,
              ),
              signingKey = key1,
              isProposal = true,
            )
          val dnd_s1_k2 =
            mkTrans(dnd_s1_k1.transaction, signingKeys = NonEmpty.mk(Set, key2), isProposal = true)
          val dnd_s1_k3 =
            mkTrans(dnd_s1_k1.transaction, signingKeys = NonEmpty.mk(Set, key3), isProposal = true)
          val dnd_s2_k1 =
            mkAdd(
              DecentralizedNamespaceDefinition.tryCreate(
                decentralizedNamespace = dnd_s1_k1.mapping.namespace,
                threshold = PositiveInt.two,
                owners = NonEmpty.mk(Set, ns1, ns2, ns3, ns6),
              ),
              signingKey = key1,
              isProposal = true,
              serial = PositiveInt.two,
            )
          // the latter two will not be the same as we are forcing the hash to be different in mkTrans
          val dnd_s2_k2p =
            mkAdd(
              DecentralizedNamespaceDefinition.tryCreate(
                decentralizedNamespace = dnd_s1_k1.mapping.namespace,
                threshold = PositiveInt.two,
                owners = NonEmpty.mk(Set, ns1, ns2, ns3, ns6),
              ),
              signingKey = key2,
              isProposal = true,
              serial = PositiveInt.two,
            )
          val dnd_s2_k6 =
            mkTrans(dnd_s2_k1.transaction, signingKeys = NonEmpty.mk(Set, key6), isProposal = true)
          val dnd_s2_k3 =
            mkTrans(dnd_s2_k1.transaction, signingKeys = NonEmpty.mk(Set, key3), isProposal = true)

          runScenarios(
            "archive competing proposals",
            List(
              ns2k2_k2,
              ns3k3_k3,
              ns3k3_k3,
              ns2k2_k2,
              dnd_s1_k1,
              dnd_s1_k2,
              dnd_s1_k3,
              dnd_s2_k6,
              dnd_s2_k2p,
              dnd_s2_k1,
              dnd_s2_k3,
            ),
          ) { store =>
            validate(
              fetch(store, ts(40), isProposal = true),
              List(),
            )
            fetchTx(store, ts(40)).result
              .map(c =>
                (c.mapping, c.transaction.signatures.map(_.signature.authorizingLongTermKey))
              )
              .filter(_._1.namespace == dnd_s1_k1.mapping.namespace) shouldBe Seq(
              (
                dnd_s2_k1.mapping,
                Set(key1, key6, key3).map(_.fingerprint),
              )
            )
          }
        }

      }

      "trigger topology subscribers with/without transactions" in {
        val (proc, store) = mkDefault()
        var testTopoSubscriberCalledEmpty: Boolean = false
        var testTopoSubscriberCalledWithTxs: Boolean = false
        val testTopoSubscriber = new TopologyTransactionProcessingSubscriber {
          override def observed(
              sequencedTimestamp: SequencedTime,
              effectiveTimestamp: EffectiveTime,
              sequencerCounter: SequencerCounter,
              transactions: Seq[GenericSignedTopologyTransaction],
          )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
            if (transactions.nonEmpty) {
              testTopoSubscriberCalledWithTxs = true
            } else {
              testTopoSubscriberCalledEmpty = true
            }
            FutureUnlessShutdown.unit
          }
        }
        val block1 = List(ns1k1_k1, dmp1_k1, ns2k2_k2, ns3k3_k3)
        val block2 = List(ns1k2_k1, dtcp1_k1)
        val block3 = List(okm1bk5k1E_k1)
        val block4 = List(dnd_proposal_k1)
        val block5 = List(dnd_proposal_k2)
        val block6 = List(dnd_proposal_k3)
        val block7 = List(ns1k1_k1)
        val block8 = List(ns1k8_k3_fail)

        process(proc, ts(0), 0, block1)
        process(proc, ts(1), 1, block2)
        process(proc, ts(2), 2, block3)
        proc.subscribe(testTopoSubscriber)
        process(proc, ts(3), 3, block4)
        clue("incomplete proposals should trigger subscriber with empty transactions") {
          testTopoSubscriberCalledWithTxs shouldBe false
          testTopoSubscriberCalledEmpty shouldBe true
        }
        testTopoSubscriberCalledEmpty = false

        process(proc, ts(4), 4, block5)
        clue("incomplete proposals should trigger subscriber with empty transactions") {
          testTopoSubscriberCalledWithTxs shouldBe false
          testTopoSubscriberCalledEmpty shouldBe true
        }
        testTopoSubscriberCalledEmpty = false

        process(proc, ts(5), 5, block6)
        clue("complete proposals should trigger subscriber with non-empty transactions") {
          testTopoSubscriberCalledWithTxs shouldBe true
          testTopoSubscriberCalledEmpty shouldBe false
        }
        testTopoSubscriberCalledWithTxs = false

        process(proc, ts(6), 6, block7)
        clue("duplicate transactions should trigger subscriber with non-empty transactions") {
          testTopoSubscriberCalledWithTxs shouldBe true
          testTopoSubscriberCalledEmpty shouldBe false
        }
        testTopoSubscriberCalledWithTxs = false

        process(proc, ts(7), 7, block8)
        clue("rejections should trigger subscriber with empty transactions") {
          testTopoSubscriberCalledWithTxs shouldBe false
          testTopoSubscriberCalledEmpty shouldBe true
        }
        testTopoSubscriberCalledEmpty = false

        val DNDafterProcessing = fetch(store, ts(5).immediateSuccessor)
          .find(_.code == TopologyMapping.Code.DecentralizedNamespaceDefinition)
          .valueOrFail("Couldn't find DND")
        DNDafterProcessing shouldBe dnd_proposal_k1.mapping
      }

      "cascading update and synchronizer parameters change" in {
        val (proc, store) = mkDefault()
        val block1 = List(ns1k1_k1, ns1k2_k1, dmp1_k2)
        process(proc, ts(0), 0, block1)
        val st1 = fetch(store, ts(0).immediateSuccessor)
        process(proc, ts(1), 1, List(Factory.mkRemoveTx(ns1k2_k1)))
        val st2 = fetch(store, ts(1).immediateSuccessor)
        validate(st1, block1)

        /*
          dmp1_k2 is not revoked
          Synchronizer governance transaction are not removed by cascading updates. The
          idea behind is that the change of synchronizer parameters is authorized and then
          the new parameters stay valid even if the authorizing key is revoked. That
          also ensures that we always have some synchronizer parameters set.
         */
        validate(st2, List(ns1k1_k1, dmp1_k2))
      }

      "fetch previous authorizations" in {
        // after a restart, we need to fetch pre-existing authorizations from our store
        // simulate this one by one
        val store = mkStore(testName = "prevauth")
        val block1 = List(ns1k1_k1, ns1k2_k1, okm1bk5k1E_k2)
        block1.zipWithIndex.foreach { case (elem, idx) =>
          val proc = mk(store)._1
          process(proc, ts(idx), idx.toLong, List(elem))
        }
        val st = fetch(store, ts(3).immediateSuccessor)
        validate(st, block1)

      }

      "correctly handle duplicate transactions" in {
        import SigningKeys.{ec as _, *}
        val dnsNamespace =
          DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns7, ns8, ns9))
        val synchronizerId =
          SynchronizerId(UniqueIdentifier.tryCreate("test-synchronizer", dnsNamespace)).toPhysical

        val dns = mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              dnsNamespace,
              PositiveInt.three,
              NonEmpty(Set, ns1, ns7, ns8, ns9),
            )
            .value,
          NonEmpty(Set, key1, key7, key8, key9),
        )

        val dopMapping = SynchronizerParametersState(
          synchronizerId,
          DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
        )
        val dop = mkAddMultiKey(
          dopMapping,
          NonEmpty(Set, key1, key7, key8),
        )

        val (proc, store) = mk(mkStore(synchronizerId, "dup"))

        def checkDop(
            ts: CantonTimestamp,
            expectedSignatures: Int,
            expectedValidFrom: CantonTimestamp,
        ) = {
          val dopInStore = store
            .findStored(ts, dop, includeRejected = false)
            .futureValueUS
            .value

          dopInStore.mapping shouldBe dopMapping
          dopInStore.transaction.signatures.forgetNE.toSeq should have size expectedSignatures.toLong
          dopInStore.validUntil shouldBe None
          dopInStore.validFrom shouldBe EffectiveTime(expectedValidFrom)
        }

        // setup
        val block0 = List[GenericSignedTopologyTransaction](
          ns1k1_k1,
          ns7k7_k7,
          ns8k8_k8,
          ns9k9_k9,
          dns,
          dop,
        )

        process(proc, ts(0), 0L, block0)
        validate(fetch(store, ts(0).immediateSuccessor), block0)
        // check that the most recently stored version after ts(0) is the one with 3 signatures
        checkDop(ts(0).immediateSuccessor, expectedSignatures = 3, expectedValidFrom = ts(0))

        val extraDop =
          mkTrans(dop.transaction, signingKeys = NonEmpty.mk(Set, key9), isProposal = true)

        // processing multiple of the same transaction in the same batch works correctly
        val block1 = List[GenericSignedTopologyTransaction](extraDop, extraDop)
        process(proc, ts(1), 1L, block1)
        validate(fetch(store, ts(1).immediateSuccessor), block0)
        // check that the most recently stored version after ts(1) is the merge of the previous one with the additional signature
        // for a total of 4 signatures
        checkDop(ts(1).immediateSuccessor, expectedSignatures = 4, expectedValidFrom = ts(1))

        // processing yet another instance of the same transaction out of batch will result in a copy of the transaction
        val block2 = List(extraDop)
        process(proc, ts(2), 2L, block2)
        validate(fetch(store, ts(2).immediateSuccessor), block0)
        // the latest transaction is now valid from ts(2)
        checkDop(ts(2).immediateSuccessor, expectedSignatures = 4, expectedValidFrom = ts(2))
      }

      "correctly handle competing proposals getting enough signatures in the same block" in {
        import SigningKeys.{ec as _, *}
        val dndNamespace = DecentralizedNamespaceDefinition.computeNamespace(Set(ns1))

        def createDnd(
            owners: Namespace*
        )(key: SigningPublicKey, serial: PositiveInt, isProposal: Boolean) =
          mkAddMultiKey(
            DecentralizedNamespaceDefinition
              .create(
                dndNamespace,
                PositiveInt.one,
                NonEmpty.from(owners).value.toSet,
              )
              .value,
            NonEmpty(Set, key),
            serial = serial,
            isProposal = isProposal,
          )

        val dnd = createDnd(ns1)(key1, serial = PositiveInt.one, isProposal = false)
        val dnd_add_ns2_k1 = createDnd(ns1, ns2)(key1, serial = PositiveInt.two, isProposal = true)
        val dnd_add_ns2_k2 = mkTrans(
          dnd_add_ns2_k1.transaction,
          signingKeys = NonEmpty.mk(Set, key2),
          isProposal = true,
        )
        val dnd_add_ns3_k1 = createDnd(ns1, ns3)(key1, serial = PositiveInt.two, isProposal = true)
        val dnd_add_ns3_k3 = mkTrans(
          dnd_add_ns3_k1.transaction,
          signingKeys = NonEmpty.mk(Set, key3),
          isProposal = true,
        )

        val (proc, store) = mkDefault()

        val rootCertificates = Seq[GenericSignedTopologyTransaction](ns1k1_k1, ns2k2_k2, ns3k3_k3)
        store
          .update(
            SequencedTime(CantonTimestamp.MinValue.immediateSuccessor),
            EffectiveTime(CantonTimestamp.MinValue.immediateSuccessor),
            removals = Map.empty,
            additions = (rootCertificates :+ dnd).map(ValidatedTopologyTransaction(_)),
          )
          .futureValueUS

        // add proposal to add ns2 to dnd signed by k2
        val block1 = List[GenericSignedTopologyTransaction](dnd_add_ns2_k2)
        process(proc, ts(1), 1L, block1)
        validate(fetch(store, ts(1).immediateSuccessor, isProposal = true), block1)

        // add proposal to add ns3 to dnd signed by k3
        val block2 = List[GenericSignedTopologyTransaction](dnd_add_ns3_k3)
        process(proc, ts(2), 2L, block2)
        // now we'll find both proposals
        validate(fetch(store, ts(2).immediateSuccessor, isProposal = true), block1 ++ block2)

        // k1 signs both proposals and processes in the same block
        val block3 = List[GenericSignedTopologyTransaction](dnd_add_ns2_k1, dnd_add_ns3_k1)
        process(proc, ts(3), 3L, block3)
        // now there should be no more proposals active
        validate(fetch(store, ts(3).immediateSuccessor, isProposal = true), Seq.empty)
        // and if we query fully authorized mappings, we should find all root certs
        // and the updated DND with ns2 as additional owner.
        // validate only looks at the mapping of dnd_add_ns2_k1 for the comparison,
        // therefore we don't have to manually merge signatures here first.
        validate(fetch(store, ts(3).immediateSuccessor), rootCertificates :+ dnd_add_ns2_k1)

        // additionally when we look up dnd_add_ns3_k1 by tx_hash,
        // we should find that it has been stored without merged signatures
        // and validFrom == validUntil.
        val rejected_dnd_add_ns3_k1 =
          store
            .findStored(CantonTimestamp.MaxValue, dnd_add_ns3_k1, includeRejected = true)
            .futureValueUS
            .value
        rejected_dnd_add_ns3_k1.transaction shouldBe dnd_add_ns3_k1
        rejected_dnd_add_ns3_k1.validUntil shouldBe Some(EffectiveTime(ts(3)))
        rejected_dnd_add_ns3_k1.validFrom shouldBe EffectiveTime(ts(3))
      }

      /* This tests the following scenario for transactions with
       * - the same mapping unique key
       * - a signature threshold of 2 to fully authorize the transaction
       *
       * 1. process transaction(serial=1, isProposal=false, signatures=2/3)
       * 2. process transaction(serial=2, isProposal=true, signatures=1/3)
       * 3. process late signature(serial=1, isProposal=false, signatures=3/3
       * 4. check that the proposal has not been expired
       * 5. process transaction(serial=2, isProposal=false, signatures=2/3)
       * 6. check that serial=2 expires serial=1
       *
       * Triggered by CN-10532
       */
      "correctly handle additional signatures after new proposals have arrived" in {
        import SigningKeys.{ec as _, *}
        val dnsNamespace =
          DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns7, ns8))
        val synchronizerId =
          SynchronizerId(UniqueIdentifier.tryCreate("test-synchronizer", dnsNamespace)).toPhysical

        val dns = mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              dnsNamespace,
              PositiveInt.two,
              NonEmpty(Set, ns1, ns7, ns8),
            )
            .value,
          NonEmpty(Set, key1, key7, key8),
        )

        // mapping and transactions for serial=1
        val dopMapping1 = SynchronizerParametersState(
          synchronizerId,
          DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
        )
        val dop1_k1k7 = mkAddMultiKey(
          dopMapping1,
          NonEmpty(Set, key1, key7),
          serial = PositiveInt.one,
        )
        val dop1_k8_late_signature =
          mkTrans(dop1_k1k7.transaction, signingKeys = NonEmpty.mk(Set, key8), isProposal = true)

        // mapping and transactions for serial=2
        val dopMapping2 = SynchronizerParametersState(
          synchronizerId,
          DynamicSynchronizerParameters
            .defaultValues(testedProtocolVersion)
            .update(
              confirmationRequestsMaxRate =
                DynamicSynchronizerParameters.defaultConfirmationRequestsMaxRate + NonNegativeInt.one
            ),
        )
        val dop2_k1_proposal =
          mkAdd(dopMapping2, signingKey = key1, serial = PositiveInt.two, isProposal = true)
        // this transaction is marked as proposal, but the merging of the signatures k1 and k7 will result
        // in a fully authorized transaction
        val dop2_k7_proposal =
          mkTrans(
            dop2_k1_proposal.transaction,
            signingKeys = NonEmpty.mk(Set, key7),
            isProposal = true,
          )

        val (proc, store) = mk(mkStore(synchronizerId, "sigs"))

        def checkDop(
            ts: CantonTimestamp,
            transactionToLookUp: GenericSignedTopologyTransaction,
            expectedSignatures: Int,
            expectedValidFrom: CantonTimestamp,
        ) = {
          val dopInStore = store
            .findStored(ts, transactionToLookUp, includeRejected = false)
            .futureValueUS
            .value

          dopInStore.mapping shouldBe transactionToLookUp.mapping
          dopInStore.transaction.signatures.forgetNE.toSeq should have size expectedSignatures.toLong
          dopInStore.validUntil shouldBe None
          dopInStore.validFrom shouldBe EffectiveTime(expectedValidFrom)
        }

        // setup: namespaces and initial mediator state
        val block0 = List[GenericSignedTopologyTransaction](
          ns1k1_k1,
          ns7k7_k7,
          ns8k8_k8,
          dns,
          dop1_k1k7,
        )
        process(proc, ts(0), 0L, block0)
        validate(fetch(store, ts(0).immediateSuccessor), block0)
        checkDop(
          ts(0).immediateSuccessor,
          transactionToLookUp = dop1_k1k7,
          expectedSignatures = 2,
          expectedValidFrom = ts(0),
        )

        // process the first proposal
        val block1 = List(dop2_k1_proposal)
        process(proc, ts(1), 1L, block1)
        validate(fetch(store, ts(1).immediateSuccessor), block0)
        // there's only the DOP proposal in the entire topology store
        validate(fetch(store, ts(1).immediateSuccessor, isProposal = true), block1)
        // we find the fully authorized transaction with 2 signatures
        checkDop(
          ts(1).immediateSuccessor,
          transactionToLookUp = dop1_k1k7,
          expectedSignatures = 2,
          expectedValidFrom = ts(0),
        )
        // we find the proposal with serial=2
        checkDop(
          ts(1).immediateSuccessor,
          transactionToLookUp = dop2_k1_proposal,
          expectedSignatures = 1,
          expectedValidFrom = ts(1),
        )

        // process the late additional signature for serial=1
        val block2 = List(dop1_k8_late_signature)
        process(proc, ts(2), 2L, block2)
        // the fully authorized mappings haven't changed since block0, only the DOP signatures
        validate(fetch(store, ts(2).immediateSuccessor), block0)
        validate(fetch(store, ts(2).immediateSuccessor, isProposal = true), block1)
        // we find the fully authorized transaction with 3 signatures
        checkDop(
          ts(2).immediateSuccessor,
          transactionToLookUp = dop1_k8_late_signature,
          expectedSignatures = 3,
          // since serial=1 got signatures updated, the updated transaction is valid as of ts(2)
          expectedValidFrom = ts(2),
        )
        // we still find the proposal. This was failing in CN-10532
        checkDop(
          ts(2).immediateSuccessor,
          transactionToLookUp = dop2_k1_proposal,
          expectedSignatures = 1,
          expectedValidFrom = ts(1),
        )

        // process another signature for serial=2 to fully authorize it
        val block3 = List(dop2_k7_proposal)
        process(proc, ts(3), 3L, block3)
        // the initial DOP mapping has now been overridden by the fully authorized serial=2 in block3
        validate(fetch(store, ts(3).immediateSuccessor), block0.init ++ block3)
        // there are no more proposals
        validate(fetch(store, ts(3).immediateSuccessor, isProposal = true), List.empty)
        // find the serial=2 mapping with 2 signatures
        checkDop(
          ts(3).immediateSuccessor,
          transactionToLookUp = dop2_k7_proposal,
          expectedSignatures = 2,
          expectedValidFrom = ts(3),
        )
        store
          .findStored(asOfExclusive = ts(3).immediateSuccessor, dop1_k1k7)
          .futureValueUS
          .value
          .validUntil
          .value
          .value shouldBe ts(3)
      }
    }
  }
}

class TopologyTransactionProcessorTestInMemory extends TopologyTransactionProcessorTest {
  protected def mkStore(
      psid: PhysicalSynchronizerId = Factory.physicalSynchronizerId1a,
      testName: String,
  ): TopologyStore[TopologyStoreId.SynchronizerStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(psid),
      testedProtocolVersion,
      loggerFactory.appendUnnamedKey("testName", testName),
      timeouts,
    )

}

class TopologyTransactionProcessorTestPostgres
    extends TopologyTransactionProcessorTest
    with DbTest
    with DbTopologyStoreHelper
    with PostgresTest

class TopologyTransactionProcessorTestH2
    extends TopologyTransactionProcessorTest
    with DbTest
    with DbTopologyStoreHelper
    with H2Test
