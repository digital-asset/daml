// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.testing.v1

import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.kms.driver.api.v1.*
import com.digitalasset.canton.crypto.{Signature, SignatureFormat, SigningKeyUsage}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.{BaseTest, HasExecutionContext, crypto}
import com.google.protobuf.ByteString
import io.opentelemetry.context.Context
import io.scalaland.chimney.dsl.*
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration.*

trait KmsDriverTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  /** Timeout for the driver to report healthy */
  protected val driverHealthyTimeout: FiniteDuration = 60.seconds

  // Override with a existing signing key ids to avoid generation of new keys
  protected val predefinedSigningKeys: Map[SigningKeySpec, String] = Map.empty

  protected val predefinedEncryptionKeys: Map[EncryptionKeySpec, String] = Map.empty

  protected val predefinedSymmetricKey: Option[String] = None

  protected lazy val emptyContext: Context = Context.root()

  // Create a new driver to be shared across the tests
  private lazy val driver: KmsDriver = newKmsDriver()

  override def afterAll(): Unit = {
    super.afterAll()
    driver.close()
  }

  /** Create a new specific KMS Driver instance */
  protected def newKmsDriver(): KmsDriver

  protected def simpleLoggerFactory(clazz: Class[_]): Logger =
    loggerFactory.getLogger(clazz).underlying

  /** Test Suite for a KMS Driver.
    *
    * A new driver is created using `newKmsDriver` if necessary.
    *
    * @param allowKeyGeneration
    *   Allow the generation of keys during the test. If false, the predefined keys have to be
    *   configured.
    */
  def kmsDriver(allowKeyGeneration: Boolean): Unit = {

    if (
      !allowKeyGeneration && (predefinedEncryptionKeys.isEmpty || predefinedSigningKeys.isEmpty || predefinedSymmetricKey.isEmpty)
    ) {
      fail("Key generation is disabled, but predefined keys not configured")
    }

    // Create a software-based crypto instance to verify signatures and asymmetrically encrypt data
    val pureCrypto =
      KmsDriverTestUtils.newPureCrypto(
        driver.supportedSigningAlgoSpecs,
        driver.supportedEncryptionAlgoSpecs,
      )

    val testData = "test".getBytes

    "report health eventually as ok" in {
      eventually(driverHealthyTimeout) {
        driver.health.futureValue shouldBe a[KmsDriverHealth.Ok.type]
      }
    }

    "report at least one supported signing key specs" in {
      driver.supportedSigningKeySpecs should not be empty
    }

    "report at least one supported signing algorithm specs" in {
      driver.supportedSigningAlgoSpecs should not be empty
    }

    "report at least one supported encryption key specs" in {
      driver.supportedEncryptionKeySpecs should not be empty
    }

    "report at least one supported encryption algorithm specs" in {
      driver.supportedEncryptionAlgoSpecs should not be empty
    }

    if (allowKeyGeneration) {
      forAll(driver.supportedSigningKeySpecs) { signingKeySpec =>
        s"generate new signing key pair with $signingKeySpec" in {
          for {
            newKeyId <- driver.generateSigningKeyPair(signingKeySpec, None)(emptyContext)
            _ <- driver.keyExistsAndIsActive(newKeyId)(emptyContext)
            _ <- driver.deleteKey(newKeyId)(emptyContext)
          } yield succeed
        }
      }

      forAll(driver.supportedEncryptionKeySpecs) { encryptionKeySpec =>
        s"generate new encryption key pair in $encryptionKeySpec" in {
          for {
            newKeyId <- driver.generateEncryptionKeyPair(encryptionKeySpec, None)(
              emptyContext
            )
            _ <- driver.keyExistsAndIsActive(newKeyId)(emptyContext)
            _ <- driver.deleteKey(newKeyId)(emptyContext)
          } yield succeed
        }
      }

      "generate new symmetric key" in {
        for {
          newKeyId <- driver.generateSymmetricKey(None)(emptyContext)
          _ <- driver.keyExistsAndIsActive(newKeyId)(emptyContext)
          _ <- driver.deleteKey(newKeyId)(emptyContext)
        } yield succeed
      }
    }

    forAll(driver.supportedSigningAlgoSpecs) { signingAlgoSpec =>
      s"sign with key and algo $signingAlgoSpec" in {
        for {
          // Find a compatible pre-defined key for the algo spec or generate a new key
          keyId <- {
            val compatibleKeySpecs =
              KmsDriverTestUtils.supportedSigningKeySpecsByAlgoSpec(signingAlgoSpec)
            predefinedSigningKeys
              .get(compatibleKeySpecs)
              .map(Future.successful)
              .getOrElse {
                if (allowKeyGeneration)
                  driver.generateSigningKeyPair(compatibleKeySpecs, None)(emptyContext)
                else
                  fail(
                    s"Key generation disabled and no key for $signingAlgoSpec defined"
                  )
              }
          }
          kmsPublicKey <- driver.getPublicKey(keyId)(emptyContext)
          kmsSignature <- driver.sign(testData, keyId, signingAlgoSpec)(emptyContext)
        } yield {
          val usage = SigningKeyUsage.ProtocolOnly
          val publicKey = KmsDriverTestUtils.signingPublicKey(kmsPublicKey, usage)
          val cryptoSigningAlgoSpec = signingAlgoSpec.transformInto[crypto.SigningAlgorithmSpec]
          val signatureFormat = SignatureFormat.fromSigningAlgoSpec(cryptoSigningAlgoSpec)
          val signature = Signature.create(
            signatureFormat,
            ByteString.copyFrom(kmsSignature),
            publicKey.id,
            Some(cryptoSigningAlgoSpec),
          )
          pureCrypto.verifySignature(
            ByteString.copyFrom(testData),
            publicKey,
            signature,
            usage,
          ) shouldBe Right(())
        }
      }
    }

    "fail to sign with unknown key id" in {
      val keyId = "invalid"
      loggerFactory.assertLogsUnorderedOptional(
        driver
          .sign(
            testData,
            keyId,
            driver.supportedSigningAlgoSpecs.headOption
              .valueOrFail("no supported signing algo specs"),
          )(emptyContext)
          .failed
          .futureValue shouldBe a[KmsDriverException],
        (
          LogEntryOptionality.Optional,
          _.warningMessage should include(
            "KMS operation `signing with key KmsKeyId(invalid)` failed"
          ),
        ),
      )
    }

    forAll(driver.supportedEncryptionAlgoSpecs) { encryptionAlgoSpec =>
      s"decrypt with key and algo $encryptionAlgoSpec" in {
        for {
          // Find a compatible pre-defined key for the algo spec or generate a new key
          keyId <- {
            val compatibleKeySpecs =
              KmsDriverTestUtils.supportedEncryptionKeySpecsByAlgoSpec(encryptionAlgoSpec)
            predefinedEncryptionKeys
              .get(compatibleKeySpecs)
              .map(Future.successful)
              .getOrElse {
                if (allowKeyGeneration)
                  driver.generateEncryptionKeyPair(compatibleKeySpecs, None)(emptyContext)
                else
                  fail(s"Key generation disabled and no key for $encryptionAlgoSpec defined")
              }
          }
          kmsPublicKey <- driver.getPublicKey(keyId)(emptyContext)
          publicKey = KmsDriverTestUtils.encryptionPublicKey(kmsPublicKey)
          testMessage = TestMessage(ByteString.copyFrom(testData))
          ciphertext = pureCrypto
            .encryptWith(testMessage, publicKey)
            .valueOrFail("encryption failed")
            .ciphertext
            .toByteArray
          plaintext <- driver.decryptAsymmetric(ciphertext, keyId, encryptionAlgoSpec)(
            emptyContext
          )
        } yield {
          plaintext shouldEqual testData
        }
      }
    }

    "symmetric encrypt and decrypt with symmetric key" in {
      for {
        // Find a compatible pre-defined key or generate a new key
        keyId <-
          predefinedSymmetricKey
            .map(Future.successful)
            .getOrElse {
              if (allowKeyGeneration)
                driver.generateSymmetricKey(None)(emptyContext)
              else
                fail("Key generation disabled and no symmetric key defined")
            }
        ciphertext <- driver.encryptSymmetric(testData, keyId)(emptyContext)
        plaintext <- driver.decryptSymmetric(ciphertext, keyId)(emptyContext)
      } yield {
        plaintext should equal(testData)
      }
    }
  }

}
