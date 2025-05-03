// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.sync

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.verifier.SyncCryptoVerifier
import com.digitalasset.canton.crypto.{
  Crypto,
  Hash,
  RequiredEncryptionSpecs,
  RequiredSigningSpecs,
  SigningKeyUsage,
  SynchronizerCryptoClient,
  TestHash,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{MemoryStorage, Storage}
import com.digitalasset.canton.topology.DefaultTestIdentities.{participant1, participant2}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  SynchronizerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

trait SyncCryptoTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  protected val sessionSigningKeysConfig: SessionSigningKeysConfig

  // Use JceCrypto for the configured crypto schemes
  private lazy val jceStaticSynchronizerParameters: StaticSynchronizerParameters =
    jceStaticSynchronizerParametersWith()

  private def jceStaticSynchronizerParametersWith(
      protocolVersion: ProtocolVersion = testedProtocolVersion
  ): StaticSynchronizerParameters = StaticSynchronizerParameters(
    requiredSigningSpecs = RequiredSigningSpecs(
      CryptoProvider.Jce.signingAlgorithms.supported,
      CryptoProvider.Jce.signingKeys.supported,
    ),
    requiredEncryptionSpecs = RequiredEncryptionSpecs(
      CryptoProvider.Jce.encryptionAlgorithms.supported,
      CryptoProvider.Jce.encryptionKeys.supported,
    ),
    requiredSymmetricKeySchemes = CryptoProvider.Jce.symmetric.supported,
    requiredHashAlgorithms = CryptoProvider.Jce.hash.supported,
    requiredCryptoKeyFormats = CryptoProvider.Jce.supportedCryptoKeyFormats,
    requiredSignatureFormats = CryptoProvider.Jce.supportedSignatureFormats,
    protocolVersion = protocolVersion,
  )

  protected lazy val otherSynchronizerId: SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("other::default")
  )

  protected lazy val testingTopology: TestingIdentityFactory =
    TestingTopology(sessionSigningKeysConfig = sessionSigningKeysConfig)
      .withSynchronizers(synchronizers = DefaultTestIdentities.synchronizerId, otherSynchronizerId)
      .withSimpleParticipants(participant1, participant2)
      // We need to use JCE-enabled static synchronizer parameters because we're using a JCE crypto provider.
      // Otherwise, validation checks will fail when attempting to sign or verify messages.
      .withStaticSynchronizerParams(jceStaticSynchronizerParameters)
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

  protected lazy val syncCryptoVerifierP1: SyncCryptoVerifier = p1.syncCryptoVerifier
  protected lazy val syncCryptoVerifierP2: SyncCryptoVerifier = p2.syncCryptoVerifier

  def syncCryptoSignerTest(): Unit = {
    "correctly sign and verify a message" in {
      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoVerifierP1
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

    "correctly sign and verify message from distinct participants" in {
      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoVerifierP2
        .verifySignature(
          testSnapshot,
          hash,
          participant1.member,
          signature,
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS
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

      syncCryptoVerifierP1
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

      syncCryptoVerifierP1
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
