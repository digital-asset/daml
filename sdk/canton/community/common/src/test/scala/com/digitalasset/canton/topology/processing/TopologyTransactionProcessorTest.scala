// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{SigningPublicKey, SynchronizerCryptoPureApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.sequencing.SubscriptionStart.FreshSubscription
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{FailOnShutdown, SequencerCounter}

abstract class TopologyTransactionProcessorTest
    extends TopologyTransactionHandlingBase
    with FailOnShutdown {

  import Factory.*

  protected def mk(
      store: TopologyStore[TopologyStoreId.SynchronizerStore] = mkStore(Factory.synchronizerId1a),
      synchronizerId: SynchronizerId = Factory.synchronizerId1a,
  ): (TopologyTransactionProcessor, TopologyStore[TopologyStoreId.SynchronizerStore]) = {

    val proc = new TopologyTransactionProcessor(
      synchronizerId,
      new SynchronizerCryptoPureApi(defaultStaticSynchronizerParameters, crypto),
      store,
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
        val (proc, store) = mk()
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
        val (proc, store) = mk()
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

      "deal with add and remove in the same block" in {
        val (proc, store) = mk()
        process(proc, ts(0), 0, List(ns1k1_k1, okm1bk5k1E_k1, Factory.mkRemoveTx(okm1bk5k1E_k1)))
        val st1 = fetch(store, ts(0).immediateSuccessor)
        validate(st1, List(ns1k1_k1))
      }

      "idempotent / crash recovery" in {
        val (proc, store) = mk()
        val block1 = List(ns1k1_k1, dmp1_k1, ns2k2_k2, ns3k3_k3)
        val block2 = List(ns1k2_k1, dtcp1_k1)
        val block3 = List(okm1bk5k1E_k1)
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

      "trigger topology subscribers with/without transactions" in {
        val (proc, store) = mk()
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
        val (proc, store) = mk()
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
        val store = mkStore()
        val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5k1E_k4)
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
          SynchronizerId(UniqueIdentifier.tryCreate("test-synchronizer", dnsNamespace))

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

        val (proc, store) = mk(mkStore(synchronizerId), synchronizerId)

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

        val extraDop = mkAdd(dopMapping, signingKey = key9, isProposal = true)

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
        val dnd_add_ns2_k2 = createDnd(ns1, ns2)(key2, serial = PositiveInt.two, isProposal = true)
        val dnd_add_ns2_k1 = createDnd(ns1, ns2)(key1, serial = PositiveInt.two, isProposal = true)
        val dnd_add_ns3_k3 = createDnd(ns1, ns3)(key3, serial = PositiveInt.two, isProposal = true)
        val dnd_add_ns3_k1 = createDnd(ns1, ns3)(key1, serial = PositiveInt.two, isProposal = true)
        val (proc, store) = mk()

        val rootCertificates = Seq[GenericSignedTopologyTransaction](ns1k1_k1, ns2k2_k2, ns3k3_k3)
        store
          .update(
            SequencedTime(CantonTimestamp.MinValue.immediateSuccessor),
            EffectiveTime(CantonTimestamp.MinValue.immediateSuccessor),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
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
          SynchronizerId(UniqueIdentifier.tryCreate("test-synchronizer", dnsNamespace))

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
          mkAdd(dopMapping1, key8, isProposal = true, serial = PositiveInt.one)

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
          mkAdd(dopMapping2, signingKey = key7, serial = PositiveInt.two, isProposal = true)

        val (proc, store) = mk(mkStore(synchronizerId), synchronizerId)

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

      /** this test checks that only fully authorized synchronizer parameter changes are used
        * to update the topology change delay for adjusting the effective time
        *
        * 0. initialize the topology store with a decentralized namespace with 2 owners
        * and default synchronizer parameters (topologyChangeDelay=250ms)
        * 1. process a proposal to update the topology change delay
        * 2. process the fully authorized update to the topology change delay
        * 3. process some other topology change delay
        *
        * only in step 3. should the updated topology change delay be used to compute the effective time
        */
      "only track fully authorized synchronizer parameter state changes" in {
        import SigningKeys.{ec as _, *}
        val dnsNamespace =
          DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns2))
        val synchronizerId =
          SynchronizerId(UniqueIdentifier.tryCreate("test-synchronizer", dnsNamespace))

        val dns = mkAddMultiKey(
          DecentralizedNamespaceDefinition
            .create(
              dnsNamespace,
              PositiveInt.two,
              NonEmpty(Set, ns1, ns2),
            )
            .value,
          signingKeys = NonEmpty(Set, key1, key2),
        )
        val initialSynchronizerParameters = mkAddMultiKey(
          SynchronizerParametersState(
            synchronizerId,
            DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
          ),
          signingKeys = NonEmpty(Set, key1, key2),
        )

        val initialTopologyChangeDelay =
          initialSynchronizerParameters.mapping.parameters.topologyChangeDelay.duration
        val updatedTopologyChangeDelay = initialTopologyChangeDelay.plusMillis(50)

        val updatedSynchronizerParams = SynchronizerParametersState(
          synchronizerId,
          DynamicSynchronizerParameters.initialValues(
            topologyChangeDelay = NonNegativeFiniteDuration.tryCreate(updatedTopologyChangeDelay),
            testedProtocolVersion,
          ),
        )
        val synchronizerParameters_k1 = mkAdd(
          updatedSynchronizerParams,
          signingKey = key1,
          serial = PositiveInt.two,
          isProposal = true,
        )
        val synchronizerParameters_k2 = mkAdd(
          updatedSynchronizerParams,
          signingKey = key2,
          serial = PositiveInt.two,
          isProposal = true,
        )

        val initialTopologyState = List(ns1k1_k1, ns2k2_k2, dns, initialSynchronizerParameters)
          .map(ValidatedTopologyTransaction(_))

        def mkEnvelope(transaction: GenericSignedTopologyTransaction) =
          Traced(
            List(
              OpenEnvelope(
                TopologyTransactionsBroadcast(
                  synchronizerId,
                  List(transaction),
                  testedProtocolVersion,
                ),
                recipients = Recipients.cc(AllMembersOfSynchronizer),
              )(testedProtocolVersion)
            )
          )

        // in block1 we propose a new topology change delay. the transaction itself will be
        // stored with the default topology change delay of 250ms and should NOT trigger a change
        // in topology change delay, because it's only a proposal
        val block1 = mkEnvelope(synchronizerParameters_k1)
        // in block2 we fully authorize the update to synchronizer parameters
        val block2 = mkEnvelope(synchronizerParameters_k2)
        // in block3 we should see the new topology change delay being used to compute the effective time
        val block3 = mkEnvelope(ns3k3_k3)

        val store = mkStore(synchronizerId)

        store
          .update(
            sequenced = SequencedTime(CantonTimestamp.MinValue.immediateSuccessor),
            effective = EffectiveTime(CantonTimestamp.MinValue.immediateSuccessor),
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = initialTopologyState,
          )
          .futureValueUS

        val (proc, _) = mk(store, synchronizerId)

        val synchronizerTimeTrackerMock = mock[SynchronizerTimeTracker]
        when(synchronizerTimeTrackerMock.awaitTick(any[CantonTimestamp])(anyTraceContext))
          .thenAnswer(None)

        proc.subscriptionStartsAt(FreshSubscription, synchronizerTimeTrackerMock).futureValueUS

        // ==================
        // process the blocks

        // block1: first proposal to update topology change delay
        // use proc.processEnvelopes directly so that the effective time is properly computed from topology change delays
        proc
          .processEnvelopes(SequencerCounter(0), SequencedTime(ts(0)), None, block1)
          .flatMap(_.unwrap)
          .futureValueUS

        // block2: second proposal to update the topology change delay, making it fully authorized
        proc
          .processEnvelopes(SequencerCounter(1), SequencedTime(ts(1)), None, block2)
          .flatMap(_.unwrap)
          .futureValueUS

        // block3: any topology transaction is now processed with the updated topology change delay
        proc
          .processEnvelopes(SequencerCounter(2), SequencedTime(ts(2)), None, block3)
          .flatMap(_.unwrap)
          .futureValueUS

        // ========================================
        // check the applied topology change delays

        // 1. fetch the proposal from block1 at a time when it has become effective
        val storedSynchronizerParametersProposal =
          fetchTx(store, ts(0).plusSeconds(1), isProposal = true)
            .collectOfMapping[SynchronizerParametersState]
            .result
            .loneElement
        // the proposal itself should be processed with the default topology change delay
        storedSynchronizerParametersProposal.validFrom.value - storedSynchronizerParametersProposal.sequenced.value shouldBe initialTopologyChangeDelay

        // 2. fetch the latest fully authorized synchronizer parameters transaction from block2 at a time when it has become effective
        val storedSynchronizerParametersUpdate = fetchTx(store, ts(1).plusSeconds(1))
          .collectOfMapping[SynchronizerParametersState]
          .result
          .loneElement
        // the transaction to change the topology change delay itself should still be processed with the default topology change delay
        storedSynchronizerParametersUpdate.validFrom.value - storedSynchronizerParametersUpdate.sequenced.value shouldBe initialTopologyChangeDelay

        // 3. fetch the topology transaction from block3 at a time when it has become effective
        val storedNSD3 = fetchTx(store, ts(2).plusSeconds(1))
          .collectOfMapping[NamespaceDelegation]
          .filter(_.mapping.namespace == ns3)
          .result
          .loneElement
        // the transaction should be processed with the updated topology change delay
        storedNSD3.validFrom.value - storedNSD3.sequenced.value shouldBe updatedTopologyChangeDelay
      }
    }
  }
}

class TopologyTransactionProcessorTestInMemory extends TopologyTransactionProcessorTest {
  protected def mkStore(
      synchronizerId: SynchronizerId = SynchronizerId(Factory.uid1a)
  ): TopologyStore[TopologyStoreId.SynchronizerStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(synchronizerId),
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )

}
class TopologyTransactionProcessorTestPostgres
    extends TopologyTransactionProcessorTest
    with DbTest
    with DbTopologyStoreHelper
    with PostgresTest {
  override protected def mkStore(
      synchronizerId: SynchronizerId
  ): TopologyStore[TopologyStoreId.SynchronizerStore] =
    createTopologyStore(synchronizerId)
}
