// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class TopologyManagerSigningKeyDetectionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  "TopologyManagerSigningKeyDetection" should {

    object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)
    import Factory.*

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mk() =
      new TopologyManagerSigningKeyDetection(
        new InMemoryTopologyStore(DomainStore(Factory.domainId1), loggerFactory, timeouts),
        Factory.cryptoApi.crypto.pureCrypto,
        Factory.cryptoApi.crypto.cryptoPrivateStore,
        loggerFactory,
      )

    def mkStored(ts: CantonTimestamp, transactions: GenericSignedTopologyTransaction*) =
      StoredTopologyTransactions(
        transactions.map(StoredTopologyTransaction(SequencedTime(ts), EffectiveTime(ts), None, _))
      )

    val dtc_uid1a = TopologyTransaction(
      Replace,
      PositiveInt.one,
      DomainTrustCertificate(ParticipantId(uid1a), domainId1),
      testedProtocolVersion,
    )

    val dtc_uid1b = TopologyTransaction(
      Replace,
      PositiveInt.one,
      DomainTrustCertificate(ParticipantId(uid1b), domainId1),
      testedProtocolVersion,
    )

    "prefer an IDD over NSD" in {

      val detector = mk()

      detector.store.bootstrap(mkStored(ts(0), ns1k1_k1, id1ak4_k1)).futureValue

      // uid1a has an identifier delegation, therefore it should be used
      detector
        .getValidSigningKeysForTransaction(ts(1), dtc_uid1a, None, returnAllValidKeys = false)
        .futureValueUS shouldBe Right(Seq(SigningKeys.key4.fingerprint))

      // uid1b has NO identifier delegation, therefore the root certificate should be used
      detector
        .getValidSigningKeysForTransaction(ts(1), dtc_uid1b, None, returnAllValidKeys = false)
        .futureValueUS shouldBe Right(Seq(SigningKeys.key1.fingerprint))

      // test geting all valid keys
      detector
        .getValidSigningKeysForTransaction(ts(1), dtc_uid1a, None, returnAllValidKeys = true)
        .futureValueUS
        .value should contain theSameElementsAs Seq(
        SigningKeys.key1,
        SigningKeys.key4,
      ).map(_.fingerprint)
    }

    "prefer keys furthest from the root certificate" in {
      val detector = mk()

      detector.store.bootstrap(mkStored(ts(0), ns1k1_k1, ns1k2_k1, ns1k3_k2)).futureValue

      detector
        .getValidSigningKeysForTransaction(ts(1), dtc_uid1a, None, returnAllValidKeys = false)
        .futureValueUS shouldBe Right(Seq(SigningKeys.key3.fingerprint))

      // test getting all valid keys
      detector
        .getValidSigningKeysForTransaction(ts(1), dtc_uid1a, None, returnAllValidKeys = true)
        .futureValueUS
        .value should contain theSameElementsAs Seq(
        SigningKeys.key1,
        SigningKeys.key2,
        SigningKeys.key3,
      ).map(_.fingerprint)

      // now let's break the chain by removing the NSD for key2.
      // this prevents key3 from being authorized for new signatures.
      detector.store
        .update(
          SequencedTime(ts(1)),
          EffectiveTime(ts(1)),
          removeMapping = Map.empty,
          removeTxs = Set(ns1k2_k1.hash),
          additions = Seq.empty,
        )
        .futureValue

      // reset caches so that the namespace delegations are fetched again from the store.
      // normally we would use a separate detector instance per topology manager srequest
      detector.reset()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        detector
          .getValidSigningKeysForTransaction(ts(2), dtc_uid1a, None, returnAllValidKeys = false)
          .futureValueUS shouldBe Right(
          Seq(SigningKeys.key1.fingerprint)
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include(
                s"The following target keys of namespace $ns1 are dangling: ${List(SigningKeys.key3.fingerprint)}"
              ),
              "dangling NSD for key3",
            )
          )
        ),
      )
    }

    "resolves decentralized namespace definitions for finding appropriate signing keys" in {
      val detector = mk()
      detector.store
        .bootstrap(mkStored(ts(0), ns1k1_k1, ns8k8_k8, ns9k9_k9, ns1k2_k1, dns1))
        .futureValue

      val otk = TopologyTransaction(
        Replace,
        PositiveInt.one,
        OwnerToKeyMapping(
          ParticipantId("decentralized-participant", dns1.mapping.namespace),
          NonEmpty(Seq, EncryptionKeys.key1, SigningKeys.key4),
        ),
        testedProtocolVersion,
      )

      cryptoApi.crypto.cryptoPrivateStore
        .removePrivateKey(SigningKeys.key8.fingerprint)
        .futureValueUS

      detector
        .getValidSigningKeysForTransaction(ts(1), otk, None, returnAllValidKeys = false)
        .futureValueUS
        .value should contain theSameElementsAs Seq(
        SigningKeys.key2, // the furthest key available for NS1
        SigningKeys.key9, // the root certificate key for NS9
        SigningKeys.key4, // all new signing keys must also sign
        // since we removed key8 from the private key store, it cannot be used to sign something, so it is not suggested
      ).map(_.fingerprint)

      detector
        .getValidSigningKeysForTransaction(ts(1), otk, None, returnAllValidKeys = true)
        .futureValueUS
        .value should contain theSameElementsAs Seq(
        SigningKeys.key1, // the root certificate key for NS1
        SigningKeys.key2, // the key authorized for NS1 by an additional NSD
        SigningKeys.key9, // the root certificate key for NS9
        SigningKeys.key4, // all new signing keys must also sign
        // since we removed key8 from the private key store, it cannot be used to sign something, so it is not suggested
      ).map(_.fingerprint)

    }

  }
}
