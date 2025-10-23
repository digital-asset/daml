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
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import monocle.macros.syntax.lens.*
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

  protected def supportedSchemes: Kms.SupportedSchemes

  private def createCryptoConfig(
      paramSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]]
  ) =
    CryptoConfig(
      provider = CryptoProvider.Kms,
      encryption = EncryptionSchemeConfig(
        algorithms = CryptoSchemeConfig(
          allowed = Some(supportedSchemes.supportedEncryptionAlgoSpecs)
        )
      ),
      signing = SigningSchemeConfig(
        algorithms = CryptoSchemeConfig(
          allowed = Some(paramSigningAlgorithmSpecs)
        )
      ),
      kms = kmsConfig,
      privateKeyStore = PrivateKeyStoreConfig(None),
    )

  lazy val cryptoConfig: CryptoConfig =
    createCryptoConfig(supportedSchemes.supportedSigningAlgoSpecs)

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
          CachingConfigs.kmsMetadataCache,
          config.privateKeyStore,
          replicaManager = None,
        ),
        testedReleaseProtocolVersion,
        futureSupervisor,
        wallClock,
        executorService,
        timeouts,
        loggerFactory,
        NoReportingTracerProvider,
      )

  lazy val kmsCryptoF: FutureUnlessShutdown[Crypto] =
    createKmsCrypto(cryptoConfig).valueOrFail("create crypto")
  lazy val kmsCryptoRestrictedF: FutureUnlessShutdown[Crypto] = createKmsCrypto(
    cryptoConfigRestricted
  ).valueOrFail("create crypto")

  "KmsCrypto" must {

    "fail if the default scheme is not supported" in {
      for {
        // we check that if a particular scheme is set as the default, but is not supported by the KMS, it fails.
        res <- createKmsCrypto(
          cryptoConfig
            .focus(_.encryption.algorithms.allowed)
            .replace(
              Some(
                NonEmpty.mk(
                  Set,
                  EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
                  EncryptionAlgorithmSpec.RsaOaepSha256,
                )
              )
            )
            .focus(_.encryption.algorithms.default)
            // EciesHkdfHmacSha256Aes128Cbc is not supported by either AWS or GCP KMS
            .replace(Some(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc))
        ).value
      } yield res.left.value should include(
        s"The configured default scheme ${EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc} not supported by " +
          s"the KMS: ${supportedSchemes.supportedEncryptionAlgoSpecs.forgetNE}"
      )
    }

    behave like signingProvider(
      supportedSchemes.supportedSigningKeySpecs,
      supportedSchemes.supportedSigningAlgoSpecs,
      CryptoProvider.Kms.supportedSignatureFormats,
      kmsCryptoF,
      Some(kmsCryptoRestrictedF),
      Some(SigningAlgorithmSpec.EcDsaSha384), // unsupported for `kmsCryptoRestrictedF`
    )

    behave like encryptionProvider(
      supportedSchemes.supportedEncryptionAlgoSpecs,
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
