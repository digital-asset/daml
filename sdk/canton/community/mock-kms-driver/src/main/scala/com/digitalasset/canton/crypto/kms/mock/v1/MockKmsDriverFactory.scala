// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{
  CachingConfigs,
  CryptoConfig,
  CryptoProvider,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriverFactory
import com.digitalasset.canton.crypto.kms.driver.v1.KmsDriverSpecsConverter
import com.digitalasset.canton.crypto.kms.mock.v1.MockKmsDriverFactory.mockKmsDriverName
import com.digitalasset.canton.crypto.provider.jce.JceCrypto
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.crypto.{CryptoSchemes, KeyName}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.version.ReleaseProtocolVersion
import org.slf4j.Logger
import pureconfig.configurable.{genericMapReader, genericMapWriter}
import pureconfig.error.CannotConvert
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

class MockKmsDriverFactory extends KmsDriverFactory {
  override type Driver = MockKmsDriver

  override def name: String = mockKmsDriverName

  override def buildInfo: Option[String] = Some(BuildInfo.version)

  override type ConfigType = MockKmsDriverConfig

  override def configReader: ConfigReader[MockKmsDriverConfig] = {
    import pureconfig.generic.semiauto.*

    implicit val mapReader = genericMapReader[KeyName, config.NonNegativeFiniteDuration] { str =>
      KeyName
        .fromProtoPrimitive(str)
        .leftMap(err => CannotConvert(str, KeyName.getClass.getName, s"Invalid key name: $err"))
    }

    deriveReader[MockKmsDriverConfig]
  }

  override def configWriter(confidential: Boolean): ConfigWriter[MockKmsDriverConfig] = {
    import pureconfig.generic.semiauto.*

    implicit val mapWriter =
      genericMapWriter[KeyName, config.NonNegativeFiniteDuration](_.toProtoPrimitive)

    deriveWriter[MockKmsDriverConfig]
  }

  // Convert the supported specs that are also supported by KMS drivers
  private def convertSpec[CS, DS](
      supported: NonEmpty[Set[CS]],
      convertFn: CS => DS,
  ): Set[DS] =
    supported.forgetNE.toList.map(convertFn(_)).toSet

  override def create(
      config: MockKmsDriverConfig,
      loggerFactory: Class[?] => Logger,
      executionContext: ExecutionContext,
  ): MockKmsDriver = {

    implicit val ec = executionContext

    // We use the Canton logging infrastructure to create a logger for the driver
    val namedLoggerFactory = NamedLoggerFactory.root
    val timeouts = ProcessingTimeout()

    // The MockKms driver supports all schemes supported by JCE.
    val cryptoConfig = CryptoConfig(provider = CryptoProvider.Jce)
    val cryptoSchemes = CryptoSchemes
      .fromConfig(cryptoConfig)
      .getOrElse(
        throw new RuntimeException("failed to validate crypto schemes from the configuration file")
      )

    val cryptoPrivateStore =
      new InMemoryCryptoPrivateStore(ReleaseProtocolVersion.latest, namedLoggerFactory)

    val cryptoPublicStore = new InMemoryCryptoPublicStore(namedLoggerFactory)

    val driverE = for {
      crypto <- JceCrypto
        .create(
          cryptoConfig,
          cryptoSchemes,
          CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
          CachingConfigs.defaultPublicKeyConversionCache,
          cryptoPrivateStore,
          cryptoPublicStore,
          timeouts,
          namedLoggerFactory,
        )
      // The Mock KMS driver supports all schemes supported by JCE
      supportedSigningKeySpecs = convertSpec(
        CryptoProvider.Jce.signingKeys.supported,
        KmsDriverSpecsConverter.convertToDriverSigningKeySpec,
      )

      supportedSigningAlgoSpecs = convertSpec(
        CryptoProvider.Jce.signingAlgorithms.supported,
        KmsDriverSpecsConverter.convertToDriverSigningAlgoSpec,
      )

      supportedEncryptionKeySpecs = convertSpec(
        CryptoProvider.Jce.encryptionKeys.supported,
        KmsDriverSpecsConverter.convertToDriverEncryptionKeySpec,
      )

      supportedEncryptionAlgoSpecs = convertSpec(
        CryptoProvider.Jce.encryptionAlgorithms.supported,
        KmsDriverSpecsConverter.convertToDriverEncryptionAlgoSpec,
      )
    } yield new MockKmsDriver(
      config,
      crypto,
      supportedSigningKeySpecs,
      supportedSigningAlgoSpecs,
      supportedEncryptionKeySpecs,
      supportedEncryptionAlgoSpecs,
      namedLoggerFactory,
    )

    driverE.valueOr { err =>
      throw new RuntimeException(s"Failed to create driver: $err")
    }
  }
}

object MockKmsDriverFactory {
  lazy val mockKmsDriverName: String = "mock-kms"
}
