// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.config.KmsConfig.ExponentialBackoffConfig
import com.digitalasset.canton.crypto.kms.driver.api.v1.{
  EncryptionAlgoSpec,
  EncryptionKeySpec,
  KmsDriver,
  KmsDriverException,
  KmsDriverHealth,
  PublicKey,
  SigningAlgoSpec,
  SigningKeySpec,
}
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.util.ByteString4096
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config, crypto}
import com.typesafe.config.ConfigValueFactory
import io.opentelemetry.context.Context
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** This test covers the non-functional properties when a KMS driver is misbehaving.
  *
  * Functionally the [[DriverKms]] is tested via the
  * [[com.digitalasset.canton.nightly.ExternalKmsTest]].
  */
class DriverKmsTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private lazy val driver = new FlakyKmsDriver()

  private lazy val kms = new DriverKms(
    KmsConfig.Driver(
      "flaky-kms",
      ConfigValueFactory.fromAnyRef(42),
      retries = KmsConfig.RetryConfig(failures =
        ExponentialBackoffConfig(
          initialDelay = config.NonNegativeFiniteDuration.ofMillis(10),
          maxDelay = config.NonNegativeDuration.ofSeconds(1),
          maxRetries = 1,
        )
      ),
    ),
    driver,
    FutureSupervisor.Noop,
    executorService,
    PositiveFiniteDuration.tryOfSeconds(5),
    wallClock,
    timeouts,
    loggerFactory,
    directExecutionContext,
  )

  private lazy val testKeyId = KmsKeyId.tryCreate("test-key")

  "DriverKms" must {

    "handle retryable exception" in {

      val err = loggerFactory.assertLogs(
        kms
          .sign(
            testKeyId,
            ByteString4096.empty,
            crypto.SigningAlgorithmSpec.EcDsaSha256,
            crypto.SigningKeySpec.EcP256,
          )
          .leftOrFailShutdown("sign")
          .futureValue,
        _.warningMessage should include(
          s"KMS operation `signing with key $testKeyId` failed: KmsSignError"
        ),
      )

      err shouldBe a[KmsError.KmsSignError]
      err.retryable shouldBe true
    }

    "handle non-retryable exception" in {

      val err =
        loggerFactory.assertLogs(
          kms
            .generateSigningKeyPair(crypto.SigningKeySpec.EcP256)
            .leftOrFailShutdown("generate signing keypair")
            .futureValue,
          _.warningMessage should include(
            "KMS operation `generate signing key pair with name N/A` failed: KmsCreateKeyError"
          ),
        )

      err shouldBe a[KmsError.KmsCreateKeyError]
      err.retryable shouldBe false
    }

    "handle unexpected exception inside a future" in {
      loggerFactory.assertLogs(
        assertThrows[RuntimeException](
          kms.getPublicSigningKey(testKeyId).failOnShutdown.futureValue
        ),
        _.warningMessage should include(
          s"KMS operation `get signing public key for $testKeyId` failed"
        ),
      )
    }

    "handle unexpected exception outside of a future" in {
      loggerFactory.assertLogs(
        assertThrows[RuntimeException](
          kms.generateSymmetricEncryptionKey().failOnShutdown.futureValue
        ),
        _.warningMessage should include(
          "KMS operation `generate symmetric encryption key with name N/A` failed"
        ),
      )
    }

    "eventually report unhealthy when the driver becomes unhealthy" in {
      driver.healthState.set(KmsDriverHealth.Failed("test"))

      eventually() {
        kms.isFailed shouldBe true
      }
    }
  }

}

private class FlakyKmsDriver()(implicit ec: ExecutionContext) extends KmsDriver {

  val healthState: AtomicReference[KmsDriverHealth] = new AtomicReference(KmsDriverHealth.Ok)

  override def health: Future[KmsDriverHealth] = Future.successful(healthState.get)

  override def supportedSigningKeySpecs: Set[SigningKeySpec] = ???

  override def supportedSigningAlgoSpecs: Set[SigningAlgoSpec] = ???

  override def supportedEncryptionKeySpecs: Set[EncryptionKeySpec] = ???

  override def supportedEncryptionAlgoSpecs: Set[EncryptionAlgoSpec] = ???

  override def sign(data: Array[Byte], keyId: String, algoSpec: SigningAlgoSpec)(
      traceContext: Context
  ): Future[Array[Byte]] =
    Future {
      // Simulate a retryable error
      throw KmsDriverException(
        new RuntimeException("signing failed"),
        retryable = true,
      )
    }

  override def generateSigningKeyPair(signingKeySpec: SigningKeySpec, keyName: Option[String])(
      traceContext: Context
  ): Future[String] =
    Future {
      // Simulate a non-retryable error
      throw KmsDriverException(
        new RuntimeException("signing keypair generation failed"),
        retryable = false,
      )
    }

  override def getPublicKey(keyId: String)(traceContext: Context): Future[PublicKey] =
    Future {
      // Simulate an unexpected exception, inside a future
      throw new RuntimeException("get public key failed")
    }

  override def generateSymmetricKey(keyName: Option[String])(
      traceContext: Context
  ): Future[String] =
    // Simulate unexpected exception outside of future
    throw new RuntimeException("generate symmetric key failed")

  override def generateEncryptionKeyPair(
      encryptionKeySpec: EncryptionKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String] = ???

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

  override def keyExistsAndIsActive(keyId: String)(traceContext: Context): Future[Unit] = ???

  override def deleteKey(keyId: String)(traceContext: Context): Future[Unit] = ???

  override def close(): Unit = ???
}
