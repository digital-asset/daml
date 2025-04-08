// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.{
  Crypto,
  Hash,
  SigningKeyUsage,
  SynchronizerCryptoClient,
  TestHash,
}
import com.digitalasset.canton.resource.{MemoryStorage, Storage}
import com.digitalasset.canton.topology.DefaultTestIdentities.{participant1, participant2}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

trait SyncCryptoSignerTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  protected val sessionSigningKeysConfig: SessionSigningKeysConfig

  protected lazy val testingTopology: TestingIdentityFactory =
    TestingTopology(sessionSigningKeysConfig = sessionSigningKeysConfig)
      .withSimpleParticipants(participant1, participant2)
      .build(crypto, loggerFactory)

  protected lazy val defaultUsage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.ProtocolOnly
  protected lazy val storage: Storage = new MemoryStorage(loggerFactory, timeouts)

  protected lazy val crypto: Crypto = Crypto
    .create(
      CryptoConfig(),
      storage,
      CryptoPrivateStoreFactory.withoutKms(wallClock, parallelExecutionContext),
      CommunityKmsFactory,
      testedReleaseProtocolVersion,
      nonStandardConfig = false,
      futureSupervisor,
      wallClock,
      executorService,
      timeouts,
      loggerFactory,
      NoReportingTracerProvider,
    )
    .valueOrFailShutdown("Failed to create crypto object")
    .futureValue

  protected lazy val testSnapshot: TopologySnapshot = testingTopology.topologySnapshot()

  protected lazy val hash: Hash = TestHash.digest(0)

  protected lazy val p1: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant1)
  protected lazy val p2: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant2)

  protected lazy val syncCryptoSignerP1: SyncCryptoSigner = p1.syncCryptoSigner
  protected lazy val syncCryptoSignerP2: SyncCryptoSigner = p2.syncCryptoSigner

  def syncCryptoSignerTest(): Unit = {
    // TODO(#23732): Add verify signature part and remaining tests (e.g. multiple signatures, etc,.)
    "correctly sign and verify a message" in {
      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoSignerP1
        .verifySignature(
          testSnapshot,
          hash,
          participant1.member,
          signature,
          defaultUsage,
        )
        .futureValueUS
        .valueOrFail("verification failed")
    }

    "correctly sign and verify multiple messages" in {

      val signature_1 = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signature_2 = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoSignerP1
        .verifySignatures(
          testSnapshot,
          hash,
          p1.member,
          NonEmpty.mk(Seq, signature_1, signature_2),
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS

    }

    "correctly sign and verify group signatures" in {

      val hash = TestHash.digest(0)

      val signature1 = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signature2 = syncCryptoSignerP2
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoSignerP1
        .verifyGroupSignatures(
          testSnapshot,
          hash,
          Seq(participant1.member, participant2.member),
          PositiveInt.two,
          "group",
          NonEmpty.mk(Seq, signature1, signature2),
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS

    }

  }

}
