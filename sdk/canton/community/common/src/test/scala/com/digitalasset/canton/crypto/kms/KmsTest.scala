// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{
  CachingConfigs,
  CryptoConfig,
  CryptoProvider,
  CryptoSchemeConfig,
  EncryptionSchemeConfig,
  SigningSchemeConfig,
}
import com.digitalasset.canton.crypto.kms.KmsError.{
  KmsDecryptError,
  KmsDeleteKeyError,
  KmsEncryptError,
  KmsSignError,
}
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.crypto.{
  CryptoPureApi,
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  EncryptionPublicKey,
  Signature,
  SignatureCheckError,
  SignatureFormat,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SigningKeyUsage,
  SigningPublicKey,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ByteString190, ByteString256, ByteString4096}
import com.digitalasset.canton.version.HasToByteString
import com.google.protobuf.ByteString
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll, FutureOutcome}

import java.util.Calendar
import scala.concurrent.ExecutionContext

trait KmsTest extends BaseTest with BeforeAndAfterAll {
  this: FixtureAsyncWordSpec =>
  type KmsType <: Kms
  override type FixtureParam = Fixture

  case class Fixture(pureCrypto: CryptoPureApi, kms: KmsType)

  override def afterAll(): Unit = {
    super.afterAll()
    defaultKms.close()
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val fixture = Fixture(pureCrypto, defaultKms)

    withFixture(test.toNoArgAsyncTest(fixture))
  }

  protected def defaultKmsConfig: KmsType#Config

  // Keep one KMS for all the tests
  protected lazy val defaultKms: KmsType = newKms(defaultKmsConfig)

  protected def newKms(config: KmsType#Config): KmsType

  val parallelExecutionContext: ExecutionContext = executionContext

  lazy val pureCrypto: CryptoPureApi = JcePureCrypto
    .create(
      CryptoConfig(
        provider = CryptoProvider.Jce,
        signing = SigningSchemeConfig(
          algorithms = CryptoSchemeConfig(Some(SigningAlgorithmSpec.EcDsaSha256)),
          keys = CryptoSchemeConfig(Some(SigningKeySpec.EcP256)),
        ),
        encryption = EncryptionSchemeConfig(
          algorithms = CryptoSchemeConfig(Some(EncryptionAlgorithmSpec.RsaOaepSha256)),
          keys = CryptoSchemeConfig(Some(EncryptionKeySpec.Rsa2048)),
        ),
      ),
      CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
      CachingConfigs.defaultPublicKeyConversionCache,
      loggerFactory,
    )
    .valueOrFail("create crypto with JCE provider")

  case class KmsSigningKey(
      keyId: KmsKeyId,
      signingKeySpec: SigningKeySpec,
      signingAlgorithmSpec: SigningAlgorithmSpec,
      signatureFormat: SignatureFormat,
  )
  case class KmsAsymmetricEncryptionKey private (
      keyId: KmsKeyId,
      encryptionKeySpec: EncryptionKeySpec,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )

  object KmsAsymmetricEncryptionKey {
    def create(
        keyId: KmsKeyId,
        encryptionKeySpec: EncryptionKeySpec,
        encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
    ): KmsAsymmetricEncryptionKey = {
      require(encryptionAlgorithmSpec.supportedEncryptionKeySpecs.contains(encryptionKeySpec))
      new KmsAsymmetricEncryptionKey(keyId, encryptionKeySpec, encryptionAlgorithmSpec)
    }
  }

  def kmsSymmetricEncryptionKeyId: FutureUnlessShutdown[KmsKeyId]
  def kmsAsymmetricEncryptionKey: FutureUnlessShutdown[KmsAsymmetricEncryptionKey]
  def kmsSigningKey: FutureUnlessShutdown[KmsSigningKey]
  // signing key used to verify a signature generated from another key
  def kmsAnotherSigningKey: FutureUnlessShutdown[KmsSigningKey]

  def convertToSigningPublicKey(
      kmsSpk: KmsSigningPublicKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): SigningPublicKey =
    kmsSpk
      .convertToSigningPublicKey(usage)
      .valueOrFail("convert public signing key")

  def convertToEncryptionPublicKey(kmsEpk: KmsEncryptionPublicKey): EncryptionPublicKey =
    kmsEpk.convertToEncryptionPublicKey
      .valueOrFail("convert public encryption key")

  lazy val plainTextData = "this_is_the_plain_text_test_data"
  lazy val dataToHandle: ByteString = ByteString.copyFrom(plainTextData.getBytes())

  case class Message(bytes: ByteString) extends HasToByteString {
    override def toByteString: ByteString = bytes
  }

  // Data that is right up to the upper bound for sign and encrypt operations.
  lazy val dataToHandle190: ByteString190 =
    ByteString190.tryCreate(ByteString.copyFrom(("t" * 190).getBytes()))
  lazy val dataToHandle4096: ByteString4096 =
    ByteString4096.tryCreate(ByteString.copyFrom(("t" * 4096).getBytes()))

  lazy val kmsKeyIdWrong: KmsKeyId = KmsKeyId(String300.tryCreate("key_wrong_id"))

  def signVerifyTest(
      pureCrypto: CryptoPureApi,
      kms: KmsType,
      signingKey: KmsSigningKey,
      tc: TraceContext = implicitly[TraceContext],
  ): FutureUnlessShutdown[Assertion] =
    for {
      signatureRaw <- kms
        .sign(
          signingKey.keyId,
          ByteString4096.tryCreate(dataToHandle),
          signingKey.signingAlgorithmSpec,
          signingKey.signingKeySpec,
        )(executionContext, tc)
        .valueOrFail("sign data")
      signatureBoundRaw <- kms
        .sign(
          signingKey.keyId,
          dataToHandle4096,
          signingKey.signingAlgorithmSpec,
          signingKey.signingKeySpec,
        )(executionContext, tc)
        .valueOrFail("sign data (right to the upper bound)")
      pubKeyKms <- kms
        .getPublicSigningKey(signingKey.keyId)(executionContext, tc)
        .valueOrFail("get public signing key from KMS key id")
      pubKey = convertToSigningPublicKey(pubKeyKms, SigningKeyUsage.ProtocolOnly)
      _ <- pureCrypto
        .verifySignature(
          dataToHandle,
          pubKey,
          Signature.create(
            signingKey.signatureFormat,
            signatureRaw,
            pubKey.id,
            Some(signingKey.signingAlgorithmSpec),
          ),
          SigningKeyUsage.ProtocolOnly,
        )
        .toEitherT[FutureUnlessShutdown]
        .valueOrFail("verify signature")
      _ <- pureCrypto
        .verifySignature(
          dataToHandle4096.unwrap,
          pubKey,
          Signature.create(
            signingKey.signatureFormat,
            signatureBoundRaw,
            pubKey.id,
            Some(signingKey.signingAlgorithmSpec),
          ),
          SigningKeyUsage.ProtocolOnly,
        )
        .toEitherT[FutureUnlessShutdown]
        .valueOrFail("verify signature for bounded data")
    } yield succeed

  def encryptDecryptSymmetricTest(
      kms: KmsType,
      keyId: KmsKeyId,
      tc: TraceContext = implicitly[TraceContext],
  ): FutureUnlessShutdown[Assertion] =
    for {
      encryptedData <- kms
        .encryptSymmetric(keyId, ByteString4096.tryCreate(dataToHandle))(executionContext, tc)
        .valueOrFail("encrypt data with symmetric key")
      encryptedDataBound <- kms
        .encryptSymmetric(keyId, dataToHandle4096)(executionContext, tc)
        .valueOrFail("encrypt data (right to the upper bound) with symmetric key")
      decryptedData <- kms
        .decryptSymmetric(keyId, encryptedData)(executionContext, tc)
        .valueOrFail("decrypt data with symmetric key")
      decryptedDataBound <- kms
        .decryptSymmetric(keyId, encryptedDataBound)(executionContext, tc)
        .valueOrFail("decrypt data (right to the upper bound) with symmetric key")
    } yield {
      encryptedData shouldNot be(dataToHandle)
      encryptedDataBound shouldNot be(dataToHandle4096)
      decryptedData should be(dataToHandle)
      decryptedDataBound should be(dataToHandle4096)
    }

  def encryptDecryptAsymmetricTest(
      pureCrypto: CryptoPureApi,
      kms: KmsType,
      keyId: KmsKeyId,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  ): FutureUnlessShutdown[Assertion] =
    for {
      publicKeyKms <- kms
        .getPublicEncryptionKey(keyId)
        .valueOrFail("get public key to encrypt")
      publicKey = convertToEncryptionPublicKey(publicKeyKms)
      encryptedData = pureCrypto
        .encryptWith(
          Message(dataToHandle),
          publicKey,
          encryptionAlgorithmSpec,
        )
        .valueOrFail("encrypt data with public key")
      encryptedDataBound = pureCrypto
        .encryptWith(
          Message(dataToHandle190.unwrap),
          publicKey,
          encryptionAlgorithmSpec,
        )
        .valueOrFail("encrypt data (right to the upper bound) with public key")
      encryptedDataCiphertext = ByteString256
        .create(encryptedData.ciphertext)
        .valueOrFail("ciphertext does not conform with bound")
      encryptedDataBoundCiphertext = ByteString256
        .create(encryptedDataBound.ciphertext)
        .valueOrFail("ciphertext does not conform with bound")
      decryptedData <- kms
        .decryptAsymmetric(keyId, encryptedDataCiphertext, encryptionAlgorithmSpec)
        .valueOrFail("decrypt data with private key")
      decryptedDataBound <- kms
        .decryptAsymmetric(keyId, encryptedDataBoundCiphertext, encryptionAlgorithmSpec)
        .valueOrFail("decrypt data (right to the upper bound) with private key")
    } yield {
      encryptedData shouldNot be(dataToHandle)
      encryptedDataBound shouldNot be(dataToHandle190)
      decryptedData should be(dataToHandle)
      decryptedDataBound should be(dataToHandle190)
    }

  def kms(): Unit = {

    "create and delete keys" in { fixture =>
      /* we only run one of the key generations, based on the day of the week, to
         prevent clogging our AWS instance with too many keys (each key takes effectively 7 days to be
         completely removed).
       */
      val dT = Calendar.getInstance()
      val dow = dT.get(Calendar.DAY_OF_WEEK)
      val generateKeyList = List[Kms => FutureUnlessShutdown[KmsKeyId]](
        kms =>
          kms
            // if AWS KMS this defaults to multiRegion = false
            .generateSymmetricEncryptionKey()
            .valueOrFail("create KMS symmetric encryption key"),
        kms =>
          kms
            .generateAsymmetricEncryptionKeyPair(EncryptionKeySpec.Rsa2048)
            .valueOrFail("create KMS asymmetric encryption key"),
        kms =>
          kms
            .generateSigningKeyPair(SigningKeySpec.EcP256)
            .valueOrFail("create KMS signing key"),
      )
      for {
        keyId <- generateKeyList(dow % generateKeyList.size)(fixture.kms).failOnShutdown
        _ <- fixture.kms
          .keyExistsAndIsActive(keyId)
          .valueOrFail("check key exists")
          .thereafterF(_ =>
            fixture.kms
              .deleteKey(keyId)
              .valueOrFailShutdown("delete key")
          )
          .failOnShutdown
        keyExists <- fixture.kms.keyExistsAndIsActive(keyId).value.failOnShutdown
      } yield keyExists.left.value should (be(a[KmsError.KmsKeyDisabledError]) or be(
        a[KmsError.KmsCannotFindKeyError]
      ))
    }

    "get public key from a KMS key identifier" in { fixture =>
      for {
        signingKey <- kmsSigningKey.failOnShutdown
        encryptionKey <- kmsAsymmetricEncryptionKey.failOnShutdown
        kmsSigningPubKey <- fixture.kms
          .getPublicSigningKey(signingKey.keyId)
          .valueOrFail("get public signing key from a KMS key id")
          .failOnShutdown
        kmsEncryptionPubKey <- fixture.kms
          .getPublicEncryptionKey(encryptionKey.keyId)
          .valueOrFail("get public encryption key from a KMS key id")
          .failOnShutdown
      } yield {
        kmsSigningPubKey.key should not be empty
        kmsEncryptionPubKey.key should not be empty
      }
    }

    "fail to get public key from a KMS key identifier for a symmetric key" in { fixture =>
      for {
        keyId <- kmsSymmetricEncryptionKeyId.failOnShutdown
        pubKey <- fixture.kms
          .getPublicEncryptionKey(keyId)
          .value
          .failOnShutdown
      } yield pubKey.left.value should be(a[KmsError.KmsGetPublicKeyError])
    }

    "encrypt and decrypt with pre-generated key" in { fixture =>
      for {
        // symmetric encryption
        symmetricKeyId <- kmsSymmetricEncryptionKeyId.failOnShutdown
        _ <- encryptDecryptSymmetricTest(fixture.kms, symmetricKeyId).failOnShutdown

        // asymmetric encryption
        asymmetricKey <- kmsAsymmetricEncryptionKey.failOnShutdown
        _ <- encryptDecryptAsymmetricTest(
          fixture.pureCrypto,
          fixture.kms,
          asymmetricKey.keyId,
          asymmetricKey.encryptionAlgorithmSpec,
        ).failOnShutdown
      } yield succeed
    }

    lazy val kmsKeyIdWrong: KmsKeyId = KmsKeyId(String300.tryCreate("key_wrong_id"))

    "fail if we encrypt and decrypt with an invalid key" in { fixture =>
      for {
        // symmetric encryption
        symmetricKeyId <- kmsSymmetricEncryptionKeyId.failOnShutdown
        encryptedDataSymmetricFailed <-
          fixture.kms
            .encryptSymmetric(kmsKeyIdWrong, ByteString4096.tryCreate(dataToHandle))
            .value
            .failOnShutdown
        encryptedDataSymmetric <- fixture.kms
          .encryptSymmetric(symmetricKeyId, ByteString4096.tryCreate(dataToHandle))
          .valueOrFail("symmetrically encrypt data")
          .failOnShutdown
        decryptedDataSymmetricFailed <- fixture.kms
          .decryptSymmetric(kmsKeyIdWrong, encryptedDataSymmetric)
          .value
          .failOnShutdown

        // asymmetric encryption
        asymmetricKeyId <- kmsAsymmetricEncryptionKey.failOnShutdown
        asymmetricKeyKms <- fixture.kms
          .getPublicEncryptionKey(asymmetricKeyId.keyId)
          .valueOrFail("get public key to encrypt")
          .failOnShutdown
        asymmetricKey = convertToEncryptionPublicKey(asymmetricKeyKms)
        encryptedDataAsymmetric =
          fixture.pureCrypto
            .encryptWith(Message(dataToHandle), asymmetricKey)
            .valueOrFail("asymmetrically encrypt data")
        encryptedDataAsymmetricCiphertext = ByteString256
          .create(encryptedDataAsymmetric.ciphertext)
          .valueOrFail("ciphertext does not conform with bound")
        decryptedDataAsymmetricFailed <- fixture.kms
          .decryptAsymmetric(
            kmsKeyIdWrong,
            encryptedDataAsymmetricCiphertext,
            encryptedDataAsymmetric.encryptionAlgorithmSpec,
          )
          .value
          .failOnShutdown
      } yield {
        encryptedDataSymmetricFailed.left.value shouldBe a[KmsEncryptError]
        decryptedDataSymmetricFailed.left.value shouldBe a[KmsDecryptError]

        decryptedDataAsymmetricFailed.left.value shouldBe a[KmsDecryptError]
      }
    }

    "sign and verify with pre-generated key" in { fixture =>
      for {
        signingKey <- kmsSigningKey.failOnShutdown
        _ <- signVerifyTest(fixture.pureCrypto, fixture.kms, signingKey).failOnShutdown
      } yield succeed
    }

    "fail if we sign with an invalid key" in { fixture =>
      for {
        signFailed <-
          fixture.kms
            // the key does not exist so the scheme selected does not matter
            .sign(
              kmsKeyIdWrong,
              ByteString4096.tryCreate(dataToHandle),
              SigningAlgorithmSpec.EcDsaSha256,
              SigningKeySpec.EcP256,
            )
            .value
            .failOnShutdown
      } yield signFailed.left.value shouldBe a[KmsSignError]
    }

    "fail if signature is invalid or we verify with the wrong key" in { fixture =>
      for {
        signingKey <- kmsSigningKey.failOnShutdown
        signingKeyWrong <- kmsAnotherSigningKey.failOnShutdown
        signatureRaw <- fixture.kms
          .sign(
            signingKey.keyId,
            ByteString4096.tryCreate(dataToHandle),
            signingKey.signingAlgorithmSpec,
            signingKey.signingKeySpec,
          )
          .valueOrFail("sign data")
          .failOnShutdown
        pubKeyKms <- fixture.kms
          .getPublicSigningKey(signingKey.keyId)
          .valueOrFail("get public signing key from KMS key id")
          .failOnShutdown
        pubKey = convertToSigningPublicKey(pubKeyKms, SigningKeyUsage.ProtocolOnly)
        signature = Signature.create(
          signingKey.signatureFormat,
          signatureRaw,
          pubKey.id,
          Some(signingKey.signingAlgorithmSpec),
        )
        wrongPubKeyKms <- fixture.kms
          .getPublicSigningKey(signingKeyWrong.keyId)
          .valueOrFail("get public signing key from KMS key id")
          .failOnShutdown
        wrongPubKey = convertToSigningPublicKey(wrongPubKeyKms, SigningKeyUsage.ProtocolOnly)
        wrongSignature = Signature.create(
          signingKey.signatureFormat,
          // add a 1 byte at the end to make the signature invalid
          signatureRaw.concat(ByteString.copyFrom(Array[Byte](1.byteValue()))),
          pubKey.id,
          Some(signingKey.signingAlgorithmSpec),
        )
        invalidSignature <- fixture.pureCrypto
          .verifySignature(dataToHandle, pubKey, wrongSignature, SigningKeyUsage.ProtocolOnly)
          .toEitherT[FutureUnlessShutdown]
          .value
          .failOnShutdown
        verifyFailed <- fixture.pureCrypto
          .verifySignature(dataToHandle, wrongPubKey, signature, SigningKeyUsage.ProtocolOnly)
          .toEitherT[FutureUnlessShutdown]
          .value
          .failOnShutdown
      } yield {
        invalidSignature.left.value shouldBe a[SignatureCheckError]
        verifyFailed.left.value shouldBe a[SignatureCheckError]
      }
    }

    "fail if deleting a non-existent key" in { fixture =>
      for {
        deleteKeyFailed <- fixture.kms.deleteKey(kmsKeyIdWrong).value.failOnShutdown
      } yield deleteKeyFailed.left.value shouldBe a[KmsDeleteKeyError]
    }

  }
}
