// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{BatchAggregatorConfig, TopologyConfig}
import com.digitalasset.canton.crypto.BaseCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.OwnerToKeyMapping
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class TopologyManagerTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)
  import Factory.*

  "TemporaryTopologyManager" should {
    behave like permittingMissingSigningKeySignatures(
      createTemporaryTopologyManager()
    )
  }
  "AuthorizedTopologyManager" should {
    behave like rejectingMissingSigningKeySignatures(
      createAuthorizedTopologyManager()
    )
  }
  "SynchronizerTopologyManager" should {
    behave like rejectingMissingSigningKeySignatures(
      createSynchronizerTopologyManager()._2
    )
    behave like ((backpressureOnFullQueue _).tupled) (
      createSynchronizerTopologyManager(maxUnsentQueueSize = NonNegativeInt.one)
    )
  }

  private def backpressureOnFullQueue(
      outbox: SynchronizerOutboxQueue,
      manager: SynchronizerTopologyManager,
  ): Unit =
    "backpressure if too many changes are pending in the outbox" in {

      val res1 = manager
        .add(Seq(ns1k1_k1), ForceFlags.none, expectFullAuthorization = true)
        .futureValueUS
      outbox.numUnsentTransactions shouldBe 1
      val res2 = manager
        .add(Seq(ns2k2_k2), ForceFlags.none, expectFullAuthorization = true)
        .futureValueUS
      outbox.numUnsentTransactions shouldBe 1
      val _ = outbox.dequeue(PositiveInt.one)
      outbox.numUnsentTransactions shouldBe 0
      val res3 = manager
        .add(Seq(ns2k2_k2), ForceFlags.none, expectFullAuthorization = true)
        .futureValueUS
      res1 shouldBe a[Right[?, ?]]
      res2 shouldBe Left(TopologyManagerError.TooManyPendingTopologyTransactions.Backpressure())
      res3 shouldBe a[Right[?, ?]]

    }

  private def permittingMissingSigningKeySignatures(
      topologyManager: TopologyManager[TopologyStoreId, BaseCrypto]
  ): Unit =
    "permit OwnerToKeyMappings with missing signing key signatures" in {
      val okmS1k7_k1_missing_k7 =
        okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value

      topologyManager
        .add(
          Seq(ns1k1_k1, okmS1k7_k1_missing_k7),
          ForceFlags.none,
          expectFullAuthorization = true,
        )
        .futureValueUS
        .value
        .unwrap
        .futureValueUS

      val tx = topologyManager.store
        .findPositiveTransactions(
          CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(Code.OwnerToKeyMapping),
          filterUid = None,
          filterNamespace = None,
        )
        .futureValueUS
        .result
        .loneElement
        .transaction

      // check that the mapping only has key7 as target key
      tx.selectMapping[OwnerToKeyMapping]
        .value
        .mapping
        .keys
        .forgetNE
        .loneElement shouldBe SigningKeys.key7

      // check that the only signature is from key1
      tx.signatures.forgetNE.loneElement.authorizingLongTermKey shouldBe SigningKeys.key1.fingerprint
    }

  private def rejectingMissingSigningKeySignatures(
      topologyManager: TopologyManager[TopologyStoreId, BaseCrypto]
  ): Unit =
    "permit OwnerToKeyMappings with missing signing key signatures" in {
      val okmS1k7_k1_missing_k7 =
        okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value

      val error = loggerFactory.assertLogs(
        topologyManager
          .add(
            Seq(ns1k1_k1, okmS1k7_k1_missing_k7),
            ForceFlags.none,
            expectFullAuthorization = true,
          )
          .swap
          .futureValueUS
          .value,
        _.shouldBeCantonError(
          TopologyManagerError.UnauthorizedTransaction,
          _ should include("Topology transaction is missing authorizations by"),
        ),
      )
      error.code shouldBe TopologyManagerError.UnauthorizedTransaction
      error.cause should include("Topology transaction is missing authorizations by")
    }

  private def createAuthorizedTopologyManager() =
    new AuthorizedTopologyManager(
      Factory.sequencer1.uid,
      wallClock,
      Factory.crypto,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting,
      new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      ),
      exitOnFatalFailures = exitOnFatal,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )

  private def createTemporaryTopologyManager() =
    new TemporaryTopologyManager(
      Factory.sequencer1.uid,
      wallClock,
      Factory.crypto,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting,
      new InMemoryTopologyStore(
        TopologyStoreId.TemporaryStore.tryCreate("test"),
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      ),
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )

  private def createSynchronizerTopologyManager(
      maxUnsentQueueSize: NonNegativeInt = NonNegativeInt.tryCreate(10)
  ) = {
    val outbox = new SynchronizerOutboxQueue(loggerFactory)
    val manager = new SynchronizerTopologyManager(
      Factory.sequencer1.uid,
      wallClock,
      Factory.syncCryptoClient.crypto,
      defaultStaticSynchronizerParameters,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting,
      new InMemoryTopologyStore(
        TopologyStoreId.SynchronizerStore(Factory.physicalSynchronizerId1),
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      ),
      outbox,
      dispatchQueueBackpressureLimit = maxUnsentQueueSize,
      disableOptionalTopologyChecks = false,
      exitOnFatalFailures = exitOnFatal,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )
    (outbox, manager)
  }

}
