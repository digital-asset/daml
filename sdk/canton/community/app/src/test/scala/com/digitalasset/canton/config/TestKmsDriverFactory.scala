// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.crypto.kms.driver.api.v1
import com.digitalasset.canton.crypto.kms.driver.api.v1.{
  EncryptionAlgoSpec,
  EncryptionKeySpec,
  KmsDriverHealth,
  PublicKey,
  SigningAlgoSpec,
  SigningKeySpec,
}
import io.opentelemetry.context.Context
import org.slf4j.Logger
import pureconfig.generic.semiauto.*
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.{ExecutionContext, Future}

class TestKmsDriverFactory extends v1.KmsDriverFactory {
  override type Driver = TestKmsDriver

  override def name: String = "test-kms"

  override def buildInfo: Option[String] = None

  override type ConfigType = TestKmsDriverConfig

  override def configReader: ConfigReader[TestKmsDriverConfig] = deriveReader[TestKmsDriverConfig]

  override def configWriter(confidential: Boolean): ConfigWriter[TestKmsDriverConfig] = { config =>
    val writer = deriveWriter[TestKmsDriverConfig]

    writer.to {
      if (confidential)
        config.copy(password = "***")
      else
        config
    }
  }

  override def create(
      config: TestKmsDriverConfig,
      loggerFactory: Class[?] => Logger,
      executionContext: ExecutionContext,
  ): TestKmsDriver = ???
}

final case class TestKmsDriverConfig(username: String, password: String)

class TestKmsDriver() extends v1.KmsDriver {
  override def health: Future[KmsDriverHealth] = ???

  override def supportedSigningKeySpecs: Set[SigningKeySpec] = ???

  override def supportedSigningAlgoSpecs: Set[SigningAlgoSpec] = ???

  override def supportedEncryptionKeySpecs: Set[EncryptionKeySpec] = ???

  override def supportedEncryptionAlgoSpecs: Set[EncryptionAlgoSpec] = ???

  override def generateSigningKeyPair(signingKeySpec: SigningKeySpec, keyName: Option[String])(
      traceContext: Context
  ): Future[String] = ???

  override def generateEncryptionKeyPair(
      encryptionKeySpec: EncryptionKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String] = ???

  override def generateSymmetricKey(keyName: Option[String])(
      traceContext: Context
  ): Future[String] = ???

  override def sign(data: Array[Byte], keyId: String, algoSpec: SigningAlgoSpec)(
      traceContext: Context
  ): Future[Array[Byte]] = ???

  override def decryptAsymmetric(
      ciphertext: Array[Byte],
      keyId: String,
      algoSpec: EncryptionAlgoSpec,
  )(traceContext: Context): Future[Array[Byte]] = ???

  override def encryptSymmetric(data: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]] = ???

  override def decryptSymmetric(ciphertext: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]] = ???

  override def getPublicKey(keyId: String)(traceContext: Context): Future[PublicKey] = ???

  override def keyExistsAndIsActive(keyId: String)(traceContext: Context): Future[Unit] = ???

  override def deleteKey(keyId: String)(traceContext: Context): Future[Unit] = ???

  override def close(): Unit = ???
}
