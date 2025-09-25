// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.validations

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CachingConfigs, CryptoConfig, CryptoProvider}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreExtended, CryptoPrivateStoreFactory}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  TestingIdentityFactory,
  TestingTopology,
}
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

/** Tests that signature verification and message decryption perform scheme validation against the
  * static synchronizer parameters. This validation is only necessary for these operations, as the
  * synchronizer handshake prevents unsupported schemes from being used for other operations such as
  * key generation, signing, or encryption.
  */
class SyncSchemeValidationsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private lazy val crypto: Crypto = Crypto
    .create(
      CryptoConfig(),
      CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
      CachingConfigs.defaultPublicKeyConversionCache,
      new MemoryStorage(loggerFactory, timeouts),
      CryptoPrivateStoreFactory.withoutKms(wallClock, parallelExecutionContext),
      testedReleaseProtocolVersion,
      futureSupervisor,
      wallClock,
      executorService,
      timeouts,
      loggerFactory,
      NoReportingTracerProvider,
    )
    .valueOrFailShutdown("Failed to create crypto object")
    .futureValue

  /*
   * Create static synchronizer parameters with restricted signing and encryption schemes
   * to test validations when verifying or decrypting with unsupported schemes.
   */
  private lazy val restrictedStaticSynchronizerParameters: StaticSynchronizerParameters =
    StaticSynchronizerParameters(
      requiredSigningSpecs = RequiredSigningSpecs(
        NonEmpty.mk(
          Set,
          SigningAlgorithmSpec.Ed25519,
          SigningAlgorithmSpec.EcDsaSha384,
        ),
        NonEmpty.mk(
          Set,
          SigningKeySpec.EcCurve25519,
          SigningKeySpec.EcP384,
        ),
      ),
      requiredEncryptionSpecs = RequiredEncryptionSpecs(
        NonEmpty.mk(
          Set,
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
        ),
        NonEmpty.mk(
          Set,
          EncryptionKeySpec.Rsa2048,
        ),
      ),
      requiredSymmetricKeySchemes = CryptoProvider.Jce.symmetric.supported,
      requiredHashAlgorithms = CryptoProvider.Jce.hash.supported,
      requiredCryptoKeyFormats = CryptoProvider.Jce.supportedCryptoKeyFormats,
      requiredSignatureFormats = NonEmpty.mk(Set, SignatureFormat.Der),
      topologyChangeDelay = StaticSynchronizerParameters.defaultTopologyChangeDelay,
      enableTransparencyChecks = false,
      protocolVersion = testedProtocolVersion,
      serial = NonNegativeInt.zero,
    )

  private lazy val testingTopology: TestingIdentityFactory =
    TestingTopology()
      .withSynchronizers(
        synchronizers = DefaultTestIdentities.physicalSynchronizerId
      )
      .withSimpleParticipants(participant1)
      .withStaticSynchronizerParams(restrictedStaticSynchronizerParameters)
      .build(crypto, loggerFactory)

  private lazy val p1: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant1)

  private lazy val hash: Hash = TestHash.digest(0)

  private lazy val testSigningKey: SigningPublicKey = p1.crypto.privateCrypto
    .generateSigningKey(
      restrictedStaticSynchronizerParameters.requiredSigningSpecs.keys.head,
      SigningKeyUsage.ProtocolOnly,
    )
    .valueOrFail("generate signing key")
    .futureValueUS

  "Fail signature verification with disallowed synchronizer schemes" in {
    // unsupported signing algorithm specifications
    (CryptoProvider.Jce.signingAlgorithms.supported
      -- restrictedStaticSynchronizerParameters.requiredSigningSpecs.algorithms)
      .foreach { unsupported =>
        val unsupportedSignature = Signature.create(
          SignatureFormat.Der,
          ByteString.EMPTY,
          Fingerprint.tryFromString("no-fingerprint"),
          Some(unsupported),
        )

        // we test with hash and hash.unwrap because the interface exposes two signature verification methods
        val res1 = p1.crypto.pureCrypto.verifySignature(
          hash,
          testSigningKey,
          unsupportedSignature,
          SigningKeyUsage.ProtocolOnly,
        )
        val res2 = p1.crypto.pureCrypto.verifySignature(
          hash.unwrap,
          testSigningKey,
          unsupportedSignature,
          SigningKeyUsage.ProtocolOnly,
        )

        res1 shouldBe a[Left[DecryptionError.UnsupportedAlgorithmSpec, _]]
        res2 shouldBe a[Left[SignatureCheckError.UnsupportedAlgorithmSpec, _]]
      }

    // unsupported signing key specifications
    (CryptoProvider.Jce.signingKeys.supported
      -- restrictedStaticSynchronizerParameters.requiredSigningSpecs.keys)
      .foreach { unsupported =>
        val signingKey = p1.crypto.privateCrypto
          .generateSigningKey(
            unsupported,
            SigningKeyUsage.ProtocolOnly,
          )
          .valueOrFail("generate signing key")
          .futureValueUS

        val signature = Signature.create(
          SignatureFormat.Der,
          ByteString.EMPTY,
          Fingerprint.tryFromString("no-fingerprint"),
          None,
        )

        // we test with hash and hash.unwrap because the interface exposes two signature verification methods
        val res1 = p1.crypto.pureCrypto.verifySignature(
          hash,
          signingKey,
          signature,
          SigningKeyUsage.ProtocolOnly,
        )
        val res2 = p1.crypto.pureCrypto.verifySignature(
          hash.unwrap,
          signingKey,
          signature,
          SigningKeyUsage.ProtocolOnly,
        )

        res1 shouldBe a[Left[SignatureCheckError.UnsupportedKeySpec, _]]
        res2 shouldBe a[Left[SignatureCheckError.UnsupportedKeySpec, _]]
      }

    // unsupported signature format
    (CryptoProvider.Jce.supportedSignatureFormats
      -- restrictedStaticSynchronizerParameters.requiredSignatureFormats).foreach { unsupported =>
      val unsupportedSignature = Signature.create(
        unsupported,
        ByteString.EMPTY,
        Fingerprint.tryFromString("no-fingerprint"),
        None,
      )

      // we test with hash and hash.unwrap because the interface exposes two signature verification methods
      val res1 = p1.crypto.pureCrypto.verifySignature(
        hash,
        testSigningKey,
        unsupportedSignature,
        SigningKeyUsage.ProtocolOnly,
      )
      val res2 = p1.crypto.pureCrypto.verifySignature(
        hash.unwrap,
        testSigningKey,
        unsupportedSignature,
        SigningKeyUsage.ProtocolOnly,
      )

      res1 shouldBe a[Left[SignatureCheckError.UnsupportedSignatureFormat, _]]
      res2 shouldBe a[Left[SignatureCheckError.UnsupportedSignatureFormat, _]]

    }

  }

  "Fail decryption with disallowed synchronizer schemes" in {

    // unsupported encryption algorithm specifications
    (CryptoProvider.Jce.encryptionAlgorithms.supported
      -- restrictedStaticSynchronizerParameters.requiredEncryptionSpecs.algorithms)
      .foreach { unsupported =>
        val encryptionKey = p1.crypto.privateCrypto
          .generateEncryptionKey(
            restrictedStaticSynchronizerParameters.requiredEncryptionSpecs.keys.head
          )
          .valueOrFail("generate encryption key")
          .futureValueUS

        val privateKey = p1.crypto.cryptoPrivateStore
          .asInstanceOf[CryptoPrivateStoreExtended]
          .exportPrivateKey(encryptionKey.id)
          .valueOrFail("export private encryption key")
          .futureValueUS
          .valueOrFail("cannot find private encryption key")
          .asInstanceOf[EncryptionPrivateKey]

        val unsupportedCiphertext =
          AsymmetricEncrypted[ByteString](ByteString.EMPTY, unsupported, encryptionKey.id)

        val res1 =
          p1.crypto.pureCrypto.decryptWith(unsupportedCiphertext, privateKey)(ct => Right(ct))
        val res2 =
          p1.crypto.privateCrypto.decrypt(unsupportedCiphertext)(ct => Right(ct)).futureValueUS

        res1 shouldBe a[Left[DecryptionError.UnsupportedAlgorithmSpec, _]]
        res2 shouldBe a[Left[DecryptionError.UnsupportedAlgorithmSpec, _]]

      }

    // unsupported encryption key specifications
    (CryptoProvider.Jce.encryptionKeys.supported
      -- restrictedStaticSynchronizerParameters.requiredEncryptionSpecs.keys)
      .foreach { unsupported =>
        val encryptionKey = p1.crypto.privateCrypto
          .generateEncryptionKey(
            unsupported
          )
          .valueOrFail("generate encryption key")
          .futureValueUS

        val privateKey = p1.crypto.cryptoPrivateStore
          .asInstanceOf[CryptoPrivateStoreExtended]
          .exportPrivateKey(encryptionKey.id)
          .valueOrFail("export private encryption key")
          .futureValueUS
          .valueOrFail("cannot find private encryption key")
          .asInstanceOf[EncryptionPrivateKey]

        val unsupportedCiphertext =
          AsymmetricEncrypted[ByteString](
            ByteString.EMPTY,
            restrictedStaticSynchronizerParameters.requiredEncryptionSpecs.algorithms.head,
            encryptionKey.id,
          )

        val res =
          p1.crypto.pureCrypto.decryptWith(unsupportedCiphertext, privateKey)(ct => Right(ct))
        // no information about the key is known within the context of 'p1.crypto.privateCrypto.decrypt' so no
        // key specification input validation is done for that function.

        res shouldBe a[Left[DecryptionError.UnsupportedKeySpec, _]]

      }

  }
}
