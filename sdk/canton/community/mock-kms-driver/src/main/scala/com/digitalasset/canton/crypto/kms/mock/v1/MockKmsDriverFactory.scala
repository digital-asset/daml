// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider, ProcessingTimeout}
import com.digitalasset.canton.crypto.KeyName
import com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriverFactory
import com.digitalasset.canton.crypto.kms.driver.v1.KmsDriverSpecsConverter
import com.digitalasset.canton.crypto.kms.mock.v1.MockKmsDriverFactory.mockKmsDriverName
import com.digitalasset.canton.crypto.provider.jce.JceCrypto
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
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
      convertFn: CS => Either[_, DS],
  ): Set[DS] =
    supported.forgetNE.toList.mapFilter(convertFn(_).toOption).toSet

  override def create(
      config: MockKmsDriverConfig,
      loggerFactory: Class[_] => Logger,
      executionContext: ExecutionContext,
  ): MockKmsDriver = {

    implicit val ec = executionContext

    // We use the Canton logging infrastructure to create a logger for the driver
    val namedLoggerFactory = NamedLoggerFactory.root
    val timeouts = ProcessingTimeout()

    val cryptoConfig = CryptoConfig(provider = CryptoProvider.Jce)

    val cryptoPrivateStore =
      new InMemoryCryptoPrivateStore(ReleaseProtocolVersion.latest, namedLoggerFactory)

    val cryptoPublicStore = new InMemoryCryptoPublicStore(namedLoggerFactory)

    val driverE = for {
      crypto <- JceCrypto
        .create(
          cryptoConfig,
          cryptoPrivateStore,
          cryptoPublicStore,
          timeouts,
          namedLoggerFactory,
        )

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
