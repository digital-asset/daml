// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{
  CachingConfigs,
  CryptoConfig,
  CryptoProvider,
  CryptoSchemeConfig,
  EncryptionSchemeConfig,
  KmsConfig,
  PrivateKeyStoreConfig,
  SigningSchemeConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

trait KmsCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with RandomTest
    with BeforeAndAfterAll
    with KmsKeysRegistration {

  protected def kmsConfig: Option[KmsConfig]

  private def createCryptoConfig(
      supportedSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]]
  ) =
    CryptoConfig(
      provider = CryptoProvider.Kms,
      encryption = EncryptionSchemeConfig(
        algorithms = CryptoSchemeConfig(
          default = Some(EncryptionAlgorithmSpec.RsaOaepSha256),
          allowed = Some(NonEmpty.mk(Set, EncryptionAlgorithmSpec.RsaOaepSha256)),
        )
      ),
      signing = SigningSchemeConfig(
        algorithms = CryptoSchemeConfig(
          default = Some(SigningAlgorithmSpec.EcDsaSha256),
          allowed = Some(supportedSigningAlgorithmSpecs),
        )
      ),
      kms = kmsConfig,
      privateKeyStore = PrivateKeyStoreConfig(None),
    )

  lazy val cryptoConfig: CryptoConfig =
    createCryptoConfig(
      NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256, SigningAlgorithmSpec.EcDsaSha384)
    )

  /* A crypto configuration with a restricted set of signing algorithm specifications to test that
   * the sign/verify function fails when called with an unsupported one (or with a key not supported
   * by any of them).
   */
  lazy val cryptoConfigRestricted: CryptoConfig =
    createCryptoConfig(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256))

  private def createKmsCrypto(config: CryptoConfig) =
    Crypto
      .create(
        config,
        CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
        CachingConfigs.defaultPublicKeyConversionCache,
        new MemoryStorage(loggerFactory, timeouts),
        new CryptoPrivateStoreFactory(
          CryptoProvider.Kms,
          kmsConfig,
          CachingConfigs.kmsMetadataCache,
          config.privateKeyStore,
          replicaManager = None,
          futureSupervisor = futureSupervisor,
          clock = wallClock,
          executionContext = executorService,
        ),
        testedReleaseProtocolVersion,
        futureSupervisor,
        wallClock,
        executorService,
        timeouts,
        loggerFactory,
        NoReportingTracerProvider,
      )
      .valueOrFail("create crypto")

  lazy val kmsCryptoF: FutureUnlessShutdown[Crypto] = createKmsCrypto(cryptoConfig)
  lazy val kmsCryptoRestrictedF: FutureUnlessShutdown[Crypto] = createKmsCrypto(
    cryptoConfigRestricted
  )

  "KmsCrypto" can {

    // We only support Ed25519 for verifying signatures, not for generating or converting keys into
    val supportedSigningAlgorithmSpecs =
      CryptoProvider.Kms.signingAlgorithms.supported
        .filterNot(_ == SigningAlgorithmSpec.Ed25519)

    behave like signingProvider(
      CryptoProvider.Kms.signingKeys.supported,
      supportedSigningAlgorithmSpecs,
      CryptoProvider.Kms.supportedSignatureFormats,
      kmsCryptoF,
      Some(kmsCryptoRestrictedF),
      Some(SigningAlgorithmSpec.EcDsaSha384), // unsupported for `kmsCryptoRestrictedF`
    )

    // We only support ECIES for encryption, not for generating or converting keys into
    val supportedEncryptionAlgorithmSpecs =
      CryptoProvider.Kms.encryptionAlgorithms.supported.filterNot(scheme =>
        Seq(
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc
        ).contains(scheme)
      )

    behave like encryptionProvider(
      supportedEncryptionAlgorithmSpecs,
      CryptoProvider.Kms.symmetric.supported,
      kmsCryptoF,
      Some(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc), // unsupported
    )
    behave like randomnessProvider(kmsCryptoF.map(_.pureCrypto))
    /*  some crypto tests are not executed for the following reasons:
     * - [[PrivateKeySerializationTest]] + [[JavaPrivateKeyConverterTest]]: the private keys are stored externally
     *    (e.g. an AWS KMS) and thus inaccessible to Canton
     */
  }

  override def afterAll(): Unit = {
    kmsCryptoRestrictedF
      .onShutdown(throw new RuntimeException("Aborted due to shutdown."))
      .foreach(_.close())
    kmsCryptoF.onShutdown(throw new RuntimeException("Aborted due to shutdown.")).foreach(_.close())
  }
}
