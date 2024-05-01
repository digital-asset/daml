// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast.Broadcast
import com.digitalasset.canton.sequencing.SubscriptionStart.FreshSubscription
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfDomain, OpenEnvelope, Recipients}
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.time.{DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

abstract class TopologyTransactionProcessorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private val crypto = new SymbolicPureCrypto()

  protected def mkStore: TopologyStore[TopologyStoreId.DomainStore]

  private def mk(
      store: TopologyStore[TopologyStoreId.DomainStore] = mkStore
  ): (TopologyTransactionProcessor, TopologyStore[TopologyStoreId.DomainStore]) = {

    val proc = new TopologyTransactionProcessor(
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
      store: TopologyStore[TopologyStoreId],
      timestamp: CantonTimestamp,
      isProposal: Boolean = false,
  ): List[TopologyMapping] = {
    fetchTx(store, timestamp, isProposal).toTopologyState
  }

  private def fetchTx(
      store: TopologyStore[TopologyStoreId],
      timestamp: CantonTimestamp,
      isProposal: Boolean = false,
  ): GenericStoredTopologyTransactions = {
    store
      .findPositiveTransactions(
        asOf = timestamp,
        asOfInclusive = false,
        isProposal = isProposal,
        types = TopologyMapping.Code.all.toSeq,
        None,
        None,
      )
      .futureValue
  }

  private def process(
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
          List(
            TopologyTransactionsBroadcast.create(
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
      observed: List[TopologyMapping],
      expected: List[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ) = {
    observed.toSet shouldBe expected.map(_.mapping).toSet
  }

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)
  import Factory.*

  "topology transaction processor" should {
    "deal with additions" in {
      val (proc, store) = mk()
      // topology processor assumes to be able to find domain parameters in the store for additional checks
      val block1 = List(ns1k1_k1, dmp1_k1)
      val block2Adds = List(ns1k2_k1, okm1bk5k1E_k1, dtcp1_k1)
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

    "idempotent / crash recovery" in {
      val (proc, store) = mk()
      val block1 = List(ns1k1_k1, dmp1_k1, ns2k2_k2, ns3k3_k3)
      val block2 = List(ns1k2_k1, dtcp1_k1)
      val block3 = List(okm1bk5k1E_k1)
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
        .find(_.code == TopologyMapping.Code.DecentralizedNamespaceDefinition)
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
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5k1E_k4)
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
      validate(st3, List(ns1k1_k1, ns1k2_k1p, id1ak4_k2, okm1bk5k1E_k4))
      validate(st4, List(ns1k1_k1, ns1k2_k1p))
      validate(st5, List(ns1k1_k1, ns1k2_k1p, id1ak4_k2p, okm1bk5k1E_k4))
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
      val block1 = List(ns1k1_k1, ns1k2_k1, id1ak4_k2, okm1bk5k1E_k4)
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
        DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns7, ns8, ns9))
      val domainId = DomainId(UniqueIdentifier.tryCreate("test-domain", dnsNamespace))

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

      val mdsMapping = MediatorDomainState
        .create(
          domainId,
          NonNegativeInt.zero,
          PositiveInt.one,
          active = Seq(DefaultTestIdentities.mediatorId),
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
      val block0 = List[GenericSignedTopologyTransaction](
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
      val block1 = List[GenericSignedTopologyTransaction](extraMds, extraMds)
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
      val domainId = DomainId(UniqueIdentifier.tryCreate("test-domain", dnsNamespace))

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
      val mdsMapping1 = MediatorDomainState
        .create(
          domainId,
          NonNegativeInt.zero,
          PositiveInt.one,
          active = Seq(DefaultTestIdentities.mediatorId),
          observers = Seq.empty,
        )
        .value
      val mds1_k1k7 = mkAddMultiKey(
        mdsMapping1,
        NonEmpty(Set, key1, key7),
        serial = PositiveInt.one,
      )
      val mds1_k8_late_signature =
        mkAdd(mdsMapping1, key8, isProposal = true, serial = PositiveInt.one)

      // mapping and transactions for serial=2
      val mdsMapping2 = MediatorDomainState
        .create(
          domainId,
          NonNegativeInt.zero,
          PositiveInt.two,
          active = Seq(
            DefaultTestIdentities.mediatorId,
            MediatorId(UniqueIdentifier.tryCreate("med2", ns7)),
          ),
          observers = Seq.empty,
        )
        .value
      val mds2_k1_proposal =
        mkAdd(mdsMapping2, signingKey = key1, serial = PositiveInt.two, isProposal = true)
      // this transaction is marked as proposal, but the merging of the signatures k1 and k7 will result
      // in a fully authorized transaction
      val mds2_k7_proposal =
        mkAdd(mdsMapping2, signingKey = key7, serial = PositiveInt.two, isProposal = true)

      val (proc, store) = mk()

      def checkMds(
          ts: CantonTimestamp,
          transactionToLookUp: GenericSignedTopologyTransaction,
          expectedSignatures: Int,
          expectedValidFrom: CantonTimestamp,
      ) = {
        val mdsInStore = store
          .findStored(ts, transactionToLookUp, includeRejected = false)
          .futureValue
          .value

        mdsInStore.mapping shouldBe transactionToLookUp.mapping
        mdsInStore.transaction.signatures.forgetNE.toSeq should have size (expectedSignatures.toLong)
        mdsInStore.validUntil shouldBe None
        mdsInStore.validFrom shouldBe EffectiveTime(expectedValidFrom)
      }

      // setup: namespaces and initial mediator state
      val block0 = List[GenericSignedTopologyTransaction](
        ns1k1_k1,
        ns7k7_k7,
        ns8k8_k8,
        dns,
        mds1_k1k7,
      )
      process(proc, ts(0), 0L, block0)
      validate(fetch(store, ts(0).immediateSuccessor), block0)
      checkMds(
        ts(0).immediateSuccessor,
        transactionToLookUp = mds1_k1k7,
        expectedSignatures = 2,
        expectedValidFrom = ts(0),
      )

      // process the first proposal
      val block1 = List(mds2_k1_proposal)
      process(proc, ts(1), 1L, block1)
      validate(fetch(store, ts(1).immediateSuccessor), block0)
      // there's only the MDS proposal in the entire topology store
      validate(fetch(store, ts(1).immediateSuccessor, isProposal = true), block1)
      // we find the fully authorized transaction with 2 signatures
      checkMds(
        ts(1).immediateSuccessor,
        transactionToLookUp = mds1_k1k7,
        expectedSignatures = 2,
        expectedValidFrom = ts(0),
      )
      // we find the proposal with serial=2
      checkMds(
        ts(1).immediateSuccessor,
        transactionToLookUp = mds2_k1_proposal,
        expectedSignatures = 1,
        expectedValidFrom = ts(1),
      )

      // process the late additional signature for serial=1
      val block2 = List(mds1_k8_late_signature)
      process(proc, ts(2), 2L, block2)
      // the fully authorized mappings haven't changed since block0, only the MDS signatures
      validate(fetch(store, ts(2).immediateSuccessor), block0)
      validate(fetch(store, ts(2).immediateSuccessor, isProposal = true), block1)
      // we find the fully authorized transaction with 3 signatures
      checkMds(
        ts(2).immediateSuccessor,
        transactionToLookUp = mds1_k8_late_signature,
        expectedSignatures = 3,
        // since serial=1 got signatures updated, the updated transaction is valid as of ts(2)
        expectedValidFrom = ts(2),
      )
      // we still find the proposal. This was failing in CN-10532
      checkMds(
        ts(2).immediateSuccessor,
        transactionToLookUp = mds2_k1_proposal,
        expectedSignatures = 1,
        expectedValidFrom = ts(1),
      )

      // process another signature for serial=2 to fully authorize it
      val block3 = List(mds2_k7_proposal)
      process(proc, ts(3), 3L, block3)
      // the initial MDS mapping has now been overridden by the fully authorized serial=2 in block3
      validate(fetch(store, ts(3).immediateSuccessor), block0.init ++ block3)
      // there are no more proposals
      validate(fetch(store, ts(3).immediateSuccessor, isProposal = true), List.empty)
      // find the serial=2 mapping with 2 signatures
      checkMds(
        ts(3).immediateSuccessor,
        transactionToLookUp = mds2_k7_proposal,
        expectedSignatures = 2,
        expectedValidFrom = ts(3),
      )
      store
        .findStored(asOfExclusive = ts(3).immediateSuccessor, mds1_k1k7)
        .futureValue
        .value
        .validUntil
        .value
        .value shouldBe ts(3)
    }

    /**  this test checks that only fully authorized domain parameter changes are used
      * to update the topology change delay for adjusting the effective time
      *
      * 0. initialize the topology store with a decentralized namespace with 2 owners
      *    and default domain parameters (topologyChangeDelay=250ms)
      * 1. process a proposal to update the topology change delay
      * 2. process the fully authorized update to the topology change delay
      * 3. process some other topology change delay
      *
      * only in step 3. should the updated topology change delay be used to compute the effective time
      */
    "only track fully authorized domain parameter state changes" in {
      import SigningKeys.{ec as _, *}
      val dnsNamespace =
        DecentralizedNamespaceDefinition.computeNamespace(Set(ns1, ns2))
      val domainId = DomainId(UniqueIdentifier.tryCreate("test-domain", dnsNamespace))

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
      val initialDomainParameters = mkAddMultiKey(
        DomainParametersState(
          domainId,
          DynamicDomainParameters.defaultValues(testedProtocolVersion),
        ),
        signingKeys = NonEmpty(Set, key1, key2),
      )

      val initialTopologyChangeDelay =
        initialDomainParameters.mapping.parameters.topologyChangeDelay.duration
      val updatedTopologyChangeDelay = initialTopologyChangeDelay.plusMillis(50)

      val updatedDomainParams = DomainParametersState(
        domainId,
        DynamicDomainParameters.initialValues(
          topologyChangeDelay = NonNegativeFiniteDuration.tryCreate(updatedTopologyChangeDelay),
          testedProtocolVersion,
        ),
      )
      val domainParameters_k1 = mkAdd(
        updatedDomainParams,
        signingKey = key1,
        serial = PositiveInt.two,
        isProposal = true,
      )
      val domainParameters_k2 = mkAdd(
        updatedDomainParams,
        signingKey = key2,
        serial = PositiveInt.two,
        isProposal = true,
      )

      val initialTopologyState = StoredTopologyTransactions(
        List(ns1k1_k1, ns2k2_k2, dns, initialDomainParameters).map(transaction =>
          StoredTopologyTransaction(
            sequenced = SequencedTime(CantonTimestamp.MinValue.immediateSuccessor),
            validFrom = EffectiveTime(CantonTimestamp.MinValue.immediateSuccessor),
            validUntil = None,
            transaction = transaction,
          )
        )
      )

      def mkEnvelope(transactions: GenericSignedTopologyTransaction) =
        Traced(
          List(
            OpenEnvelope(
              TopologyTransactionsBroadcast.create(
                domainId,
                Seq(Broadcast(String255("topology request id")(), List(transactions))),
                testedProtocolVersion,
              ),
              recipients = Recipients.cc(AllMembersOfDomain),
            )(testedProtocolVersion)
          )
        )

      // in block1 we propose a new topology change delay. the transaction itself will be
      // stored with the default topology change delay of 250ms and should NOT trigger a change
      // in topology change delay, because it's only a proposal
      val block1 = mkEnvelope(domainParameters_k1)
      // in block2 we fully authorize the update to domain parameters
      val block2 = mkEnvelope(domainParameters_k2)
      // in block3 we should see the new topology change delay being used to compute the effective time
      val block3 = mkEnvelope(ns3k3_k3)

      val store = mkStore
      store.bootstrap(initialTopologyState).futureValue

      val (proc, _) = mk(store)

      val domainTimeTrackerMock = mock[DomainTimeTracker]
      when(domainTimeTrackerMock.awaitTick(any[CantonTimestamp])(anyTraceContext)).thenAnswer(None)

      proc.subscriptionStartsAt(FreshSubscription, domainTimeTrackerMock).futureValueUS

      // ==================
      // process the blocks

      // block1: first proposal to update topology change delay
      // use proc.processEnvelopes directly so that the effective time is properly computed from topology change delays
      proc
        .processEnvelopes(SequencerCounter(0), SequencedTime(ts(0)), block1)
        .flatMap(_.unwrap)
        .futureValueUS

      // block2: second proposal to update the topology change delay, making it fully authorized
      proc
        .processEnvelopes(SequencerCounter(1), SequencedTime(ts(1)), block2)
        .flatMap(_.unwrap)
        .futureValueUS

      // block3: any topology transaction is now processed with the updated topology change delay
      proc
        .processEnvelopes(SequencerCounter(2), SequencedTime(ts(2)), block3)
        .flatMap(_.unwrap)
        .futureValueUS

      // ========================================
      // check the applied topology change delays

      // 1. fetch the proposal from block1 at a time when it has become effective
      val storedDomainParametersProposal = fetchTx(store, ts(0).plusSeconds(1), isProposal = true)
        .collectOfMapping[DomainParametersState]
        .result
        .loneElement
      // the proposal itself should be processed with the default topology change delay
      storedDomainParametersProposal.validFrom.value - storedDomainParametersProposal.sequenced.value shouldBe initialTopologyChangeDelay

      // 2. fetch the latest fully authorized domain parameters transaction from block2 at a time when it has become effective
      val storedDomainParametersUpdate = fetchTx(store, ts(1).plusSeconds(1))
        .collectOfMapping[DomainParametersState]
        .result
        .loneElement
      // the transaction to change the topology change delay itself should still be processed with the default topology change delay
      storedDomainParametersUpdate.validFrom.value - storedDomainParametersUpdate.sequenced.value shouldBe initialTopologyChangeDelay

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

class TopologyTransactionProcessorTestInMemory extends TopologyTransactionProcessorTest {
  protected def mkStore: TopologyStore[TopologyStoreId.DomainStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
    )

}
class TopologyTransactionProcessorTestPostgres
    extends TopologyTransactionProcessorTest
    with DbTest
    with DbTopologyStoreHelper
    with PostgresTest {
  override protected def mkStore: TopologyStore[TopologyStoreId.DomainStore] =
    createTopologyStore()
}
